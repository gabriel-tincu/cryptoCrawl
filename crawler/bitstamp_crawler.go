package crawler

import (
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/toorop/go-pusher"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	bitStampPairMapping = map[string]string{
		BTCUSD: BTCUSD,
		ETHUSD: ETHUSD,
		BTCEUR: BTCEUR,
		ETHEUR: ETHEUR,
	}
)

const (
	bitstampTickerURL    = "https://www.bitstamp.net/api/ticker/"
	bitstamp             = "bitstamp"
	bitStampAppId        = "de504dc5763aeef9ff52"
	bitStampTradeChannel = "live_trades_%s"
	bitStampOrderChannel = "diff_order_book_%s"
	bitStampUrlFormat    = "https://www.bitstamp.net/api/v2/transactions/%s/"
)

type BitStampCrawler struct {
	pairs      []string
	state      sync.Map
	writer     DataWriter
	httpClient http.Client
	client     pusher.Client
	tradeChan  chan *pusher.Event
	orderChan  chan *pusher.Event
	timeDiff   int64
}

func push() {
	c, err := pusher.NewClient("de504dc5763aeef9ff52")
	if err != nil {
		panic(err)
	}
	err = c.Subscribe("live_trades_ethusd")
	if err != nil {
		panic(err)
	}
	err = c.Subscribe("diff_order_book")
	if err != nil {
		panic(err)
	}
	order, err := c.Bind("trade")
	if err != nil {
		panic(err)
	}
	for e := range order {
		fmt.Printf("%+v\n", e)
	}
}
func NewBitStamp(writer DataWriter, pairs []string) (*BitStampCrawler, error) {
	cli, err := pusher.NewClient(bitStampAppId)
	if err != nil {
		return nil, err
	}
	for _, p := range pairs {
		v, ok := bitStampPairMapping[p]
		if !ok {
			return nil, fmt.Errorf("invalid mapping: %s", p)
		}
		cli.Subscribe(fmt.Sprintf(bitStampTradeChannel, strings.ToLower(v)))
		cli.Subscribe(fmt.Sprintf(bitStampOrderChannel, strings.ToLower(v)))
	}
	tc, err := cli.Bind("trade")
	if err != nil {
		return nil, err
	}
	oc, err := cli.Bind("data")
	if err != nil {
		return nil, err
	}
	timeServ, err := getBitStampTime()
	if err != nil {
		return nil, err
	}
	log.Infof("got a time diff of %d, with local time: %+v", timeServ-time.Now().Unix(), time.Now())
	return &BitStampCrawler{
		client:     *cli,
		writer:     writer,
		pairs:      pairs,
		httpClient: http.Client{},
		state:      sync.Map{},
		tradeChan:  tc,
		orderChan:  oc,
		timeDiff:   timeServ - time.Now().Unix(),
	}, nil
}

func (c *BitStampCrawler) Loop() {
	for {
		select {
		case t := <-c.tradeChan:
			for k, v := range bitStampPairMapping {
				if strings.Contains(t.Channel, strings.ToLower(k)) {
					tr := BitstampStreamTrade{}
					err := json.Unmarshal([]byte(t.Data), &tr)
					if err != nil {
						log.Error(err)
						continue
					} else {
						c.handleTrade(v, tr)
					}
				}
			}
		case o := <-c.orderChan:
			for k, v := range bitStampPairMapping {
				if strings.Contains(o.Channel, strings.ToLower(k)) {
					or := BitstampStreamOrder{}
					err := json.Unmarshal([]byte(o.Data), &or)
					if err != nil {
						log.Error(err)
						continue
					} else {
						c.handleOrder(v, or)
					}
				}
			}

		}
	}
}

func (c *BitStampCrawler) LoopRest() {
	tick := time.Tick(time.Second * 4)
	for {
		select {
		case <-tick:
			for _, p := range c.pairs {
				go c.handle(p)
			}
		}
	}
}

func (c *BitStampCrawler) handleTrade(pair string, tr BitstampStreamTrade) {
	trans := buy
	if tr.Type == 1 {
		trans = sell
	}
	m := TradeMeasurement{
		Platform:        bitstamp,
		Timestamp:       tr.Timestamp,
		Price:           tr.Price,
		Amount:          tr.Amount,
		Meta:            trade,
		Pair:            pair,
		TradeType:       limit,
		TransactionType: trans,
	}
	c.writer.Write(m)
}

func (c *BitStampCrawler) handleOrder(pair string, or BitstampStreamOrder) {
	for i, b := range or.Bids {
		m := OrderMeasurement{
			Amount:    b.Amount,
			Price:     b.Price,
			Pair:      pair,
			Timestamp: or.Timestamp + int64(i) - c.timeDiff,
			Meta:      order,
			Platform:  bitstamp,
			Type:      buy,
		}
		c.writer.Write(m)
	}
	for i, a := range or.Asks {
		m := OrderMeasurement{
			Amount:    a.Amount,
			Price:     a.Price,
			Pair:      pair,
			Timestamp: or.Timestamp + int64(i) - c.timeDiff,
			Meta:      order,
			Platform:  bitstamp,
			Type:      sell,
		}
		c.writer.Write(m)
	}
}

// deprecated
func (c *BitStampCrawler) handle(pair string) {
	if v, ok := bitStampPairMapping[pair]; ok {
		trades, err := c.Trades(pair)
		if err != nil {
			log.Errorf("error retrieving trades: %s", err)
		}
		if len(trades) == 0 {
			log.Infof("no trades retrieved")
			return
		}
		var lastTime int64
		if lt, ok := c.state.Load(lastTrade + pair); ok {
			lastTime = lt.(int64)
		} else {
			log.Warnf("last timestamp for %s not found", pair)
			lastTime = 0
		}
		c.state.Store(lastTrade+pair, trades[0].Timestamp)
		if lastTime != 0 {
			trades = trades[:len(trades)-1]
		}
		for _, tr := range trades {
			if tr.Timestamp < lastTime {
				log.Infof("bailing out at timestamp %d", lastTime)
				break
			}
			trans := buy
			if tr.Type == 1 {
				trans = sell
			}
			m := TradeMeasurement{
				Platform:        bitstamp,
				Timestamp:       tr.Timestamp,
				Price:           tr.Price,
				Amount:          tr.Amount,
				Meta:            trade,
				Pair:            v,
				TradeType:       limit,
				TransactionType: trans,
			}
			c.writer.Write(m)
		}
	} else {
		log.Errorf("unable to find mapping for %s", pair)
	}
}

func (c *BitStampCrawler) Trades(pair string) ([]BitstampTrade, error) {
	fullUrl := fmt.Sprintf(bitStampUrlFormat, strings.ToLower(pair))
	resp, err := c.httpClient.Get(fullUrl)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	trades := []BitstampTrade{}
	byts, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(byts, &trades)
	if err != nil {
		return nil, err
	}
	return trades, nil
}

type BitstampStreamTrade struct {
	Amount      float64 `json:"amount"`
	BuyOrderId  int64   `json:"buy_order_id"`
	SellOrderId int64   `json:"sell_order_id"`
	Price       float64 `json:"price"`
	Timestamp   int64   `json:"timestamp,string"`
	Id          int64   `json:"id"`
	Type        int64   `json:"type"`
}

type BitstampStreamOrder struct {
	Timestamp int64       `json:"timestamp,string"`
	Bids      []OrderData `json:"bids,string"`
	Asks      []OrderData `json:"asks,string"`
}

type BitstampTickerResponse struct {
	Timestamp int64 `json:"timestamp,string"`
}

type BitstampTrade struct {
	Timestamp int64   `json:"date,string"`
	Tid       int64   `json:"tid,string"`
	Price     float64 `json:"price,string"`
	Amount    float64 `json:"amount,string"`
	Type      int     `json:"type,string"`
}

type OrderData struct {
	Price  float64
	Amount float64
}

func (o *OrderData) UnmarshalJSON(data []byte) error {
	var str []string
	err := json.Unmarshal(data, &str)
	if err != nil {
		return err
	}
	if len(str) != 2 {
		return fmt.Errorf("malformed input: %s", string(data))
	}
	o.Price, err = strconv.ParseFloat(str[0], 64)
	if err != nil {
		return err
	}
	o.Amount, err = strconv.ParseFloat(str[1], 64)
	if err != nil {
		return nil
	}
	return nil
}

func getBitStampTime() (int64, error) {
	r, err := http.Get(bitstampTickerURL)
	if err != nil {
		return 0, err
	}
	defer r.Body.Close()
	bits, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return 0, err
	}
	var resp BitstampTickerResponse
	err = json.Unmarshal(bits, &resp)
	if err != nil {
		return 0, err
	}
	log.Infof("got ticker response : %+v", time.Unix(resp.Timestamp, 0))
	return resp.Timestamp, nil
}
