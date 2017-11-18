package crawler

import (
	"github.com/pusher/pusher-http-go"
	log "github.com/sirupsen/logrus"
	"net/http"
	"fmt"
	"strings"
	"encoding/json"
	"io/ioutil"
	"time"
	"sync"
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
	bitstamp             = "bitstamp"
	bitStampAppId        = "de504dc5763aeef9ff52"
	bitStampTradeChannel = "live_trades"
	bitStampOrderChannel = "order_book"
	bitStampUrlFormat    = "https://www.bitstamp.net/api/v2/transactions/%s/"
)

type BitStampCrawler struct {
	pairs  []string
	state sync.Map
	writer DataWriter
	httpClient http.Client
	client pusher.Client
}

func NewBitStamp(writer DataWriter, pairs []string) (BitStampCrawler, error) {
	cli := pusher.Client{
		AppId: bitStampAppId,
	}
	return BitStampCrawler{
		client: cli,
		writer: writer,
		pairs:  pairs,
		httpClient: http.Client{},
		state:sync.Map{},
	}, nil
}

func (c *BitStampCrawler) Loop() {
	tick := time.Tick(time.Second*4)
	for {
		select {
		case <- tick:
			for _, p := range c.pairs {
				go c.handle(p)
			}
		}
	}
}

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
		if lt, ok := c.state.Load(lastTrade+pair); ok {
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
				Platform:  bitstamp,
				Timestamp: tr.Timestamp,
				Price:     tr.Price,
				Amount:    tr.Amount,
				Meta:      trade,
				Pair:v,
				TradeType:limit,
				TransactionType:trans,
			}
			err = c.writer.Write(m)
			if err != nil {
				log.Error(err)
			}
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

type BitstampTrade struct {
	Timestamp int64   `json:"date,string"`
	Tid       int64   `json:"tid,string"`
	Price     float64 `json:"price,string"`
	Amount    float64 `json:"amount,string"`
	Type      int     `json:"type,string"`
}
