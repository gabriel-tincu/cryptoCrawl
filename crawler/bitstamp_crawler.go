package crawler

import (
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/toorop/go-pusher"
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
	bitStampAppId        = "de504dc5763aeef9ff52"
	bitStampTradeChannel = "live_trades_%s"
	bitStampOrderChannel = "diff_order_book_%s"
	bitStampUrlFormat    = "https://www.bitstamp.net/api/v2/transactions/%s/"
)

type BitStampCrawler struct {
	pairs      []string
	state      sync.Map
	writers    []DataWriter
	httpClient http.Client
	client     pusher.Client
	tradeChan  chan *pusher.Event
	orderChan  chan *pusher.Event
	timeDiff   int64
}

func NewBitStamp(writers []DataWriter, pairs []string) (Crawler, error) {
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
		writers:    writers,
		pairs:      pairs,
		httpClient: http.Client{},
		state:      sync.Map{},
		tradeChan:  tc,
		orderChan:  oc,
		timeDiff:   timeServ - time.Now().Unix(),
	}, nil
}

func (c *BitStampCrawler) Close() {}

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



func (c *BitStampCrawler) handleTrade(pair string, tr BitstampStreamTrade) {
	trans := buy
	if tr.Type == 1 {
		trans = sell
	}
	m := TradeMeasurement{
		Platform:        Bitstamp,
		Timestamp:       time.Now().Unix(),
		Price:           tr.Price,
		Amount:          tr.Amount,
		Meta:            trade,
		Pair:            pair,
		TradeType:       limit,
		TransactionType: trans,
	}
	for _, w := range c.writers {
		w.Write(m)
	}
}

func (c *BitStampCrawler) handleOrder(pair string, or BitstampStreamOrder) {
	for i, b := range or.Bids {
		m := OrderMeasurement{
			Amount:    b.Amount,
			Price:     b.Price,
			Pair:      pair,
			Timestamp: Now() - int64(i),
			Meta:      order,
			Platform:  Bitstamp,
			Type:      buy,
		}
		for _, w := range c.writers {
			w.Write(m)
		}
	}
	for i, a := range or.Asks {
		m := OrderMeasurement{
			Amount:    a.Amount,
			Price:     a.Price,
			Pair:      pair,
			Timestamp: Now()-int64(i),
			Meta:      order,
			Platform:  Bitstamp,
			Type:      sell,
		}
		for _, w := range c.writers {
			w.Write(m)
		}
	}
}


func (c *BitStampCrawler) Trades(pair string) ([]BitstampTrade, error) {
	fullUrl := fmt.Sprintf(bitStampUrlFormat, strings.ToLower(pair))
	resp, err := c.httpClient.Get(fullUrl)
	if err != nil {
		return nil, err
	}
	trades := []BitstampTrade{}
	err = ReadJson(resp, &trades)
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
	var resp BitstampTickerResponse
	err = ReadJson(r, &resp)
	if err != nil {
		return 0, err
	}
	log.Infof("got ticker response : %+v", time.Unix(resp.Timestamp, 0))
	return resp.Timestamp, nil
}
