package crawler

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"
)

type DataWriter interface {
	Write(interface{})
}

const (
	ETHUSD = "ETHUSD"
	ETHEUR = "ETHEUR"
	BTCUSD = "BTCUSD"
	BTCEUR = "BTCEUR"
	BCHUSD = "BCHUSD"
	BCHEUR = "BCHEUR"
	XMRUSD = "XMRUSD"
	XMREUR = "XMREUR"
	LTCUSD = "LTCUSD"
	LTCEUR = "LTCEUR"
	ETCUSD = "ETCUSD"
	ETCEUR = "ETCEUR"
	XRPUSD = "XRPUSD"
	XRPEUR = "XRPEUR"

	lastTrade = "lastTrade"
	market    = "market"
	limit     = "limit"
	amount    = "amount"
	rate      = "rate"
	tip       = "type"
	data      = "data"
	ask       = "ask"
	bid       = "bid"
	buy       = "buy"
	sell      = "sell"
	pair      = "pair"
	trade     = "trade"
	order     = "order"
	cancel    = "cancel"

	HitBTC   = "hitbtc"
	Kraken   = "kraken"
	Poloniex = "poloniex"
	Bitstamp = "bitstamp"
	Bitfin   = "bitfinex"
	Bittrex  = "bittrex"
	Binance  = "binance"
	Quione   = "quione"
	Bitthumb = "bitthumb"
	Coinone  = "coinone"
)

type InfluxMeasurement struct {
	Measurement string
	Tags        map[string]string
	Fields      map[string]interface{}
	Timestamp   time.Time
}

type CancelMeasurement struct {
	Platform  string  `json:"platform"`
	Meta      string  `json:"meta"`
	Type      string  `json:"type"`
	Pair      string  `json:"pair"`
	Price     float64 `json:"price"`
	TimeStamp int64   `json:"time"`
}

func (c CancelMeasurement) AsInfluxMeasurement() InfluxMeasurement {
	return InfluxMeasurement{
		Measurement: c.Meta,
		Tags:        map[string]string{"pair": c.Pair, "type": c.Type, "platform": c.Platform},
		Fields:      map[string]interface{}{"price": c.Price},
		Timestamp:   time.Unix(c.TimeStamp/1000, 0),
	}
}

type OrderMeasurement struct {
	Meta string `json:"meta"`
	// buy or sell
	Type string `json:"type"`
	// pair name - normalized
	Pair string `json:"pair"`
	// platform name
	Platform  string  `json:"platform"`
	Amount    float64 `json:"amount"`
	Price     float64 `json:"price"`
	Timestamp int64   `json:"time"`
}

func (o OrderMeasurement) AsInfluxMeasurement() InfluxMeasurement {
	return InfluxMeasurement{
		Measurement: o.Meta,
		Tags:        map[string]string{"pair": o.Pair, "type": o.Type, "platform": o.Platform},
		Fields:      map[string]interface{}{"price": o.Price, "amount": o.Amount},
		Timestamp:   time.Unix(o.Timestamp/1000, 0),
	}
}

type TradeMeasurement struct {
	Meta     string `json:"meta"`
	Pair     string `json:"pair"`
	Platform string `json:"platform"`
	// market, limit
	TradeType string  `json:"trade_type"`
	Amount    float64 `json:"amount"`
	Price     float64 `json:"price"`
	// buy, sell
	TransactionType string `json:"type"`
	Timestamp       int64  `json:"time"`
}

func (o TradeMeasurement) AsInfluxMeasurement() InfluxMeasurement {
	return InfluxMeasurement{
		Measurement: o.Meta,
		Tags:        map[string]string{"pair": o.Pair, "platform": o.Platform, "trade_type": o.TradeType, "type": o.TransactionType},
		Fields:      map[string]interface{}{"price": o.Price, "amount": o.Amount},
		Timestamp:   time.Unix(o.Timestamp/1000, 0),
	}
}

type CrawlerFactory func(writers []DataWriter, pairs []string) (Crawler, error)

type Crawler interface {
	Loop()
	Close()
}

type InfluxIngestable interface {
	AsInfluxMeasurement() InfluxMeasurement
}

type CrawlerConfig struct {
	Name  string   `json:"name"`
	Pairs []string `json:"pairs"`
}

type CustomTime struct {
	Time time.Time
}

func (c *CustomTime) UnmarshalJSON(b []byte) (err error) {
	s := strings.Trim(string(b), "\"")
	t, err := time.Parse("2006-01-02 15:04:05", s)
	if err != nil {
		return err
	}
	c.Time = t
	return nil
}

type OrderData struct {
	Timestamp int64
	Price     float64
	Amount    int64
	Pair      string
}

type OrderFilter struct {
	locker sync.Mutex
	orders []OrderData
}

func NewOrderFiler() OrderFilter {
	var orders []OrderData
	return OrderFilter{
		orders: orders,
		locker: sync.Mutex{},
	}
}

func (f *OrderFilter) FilterOrders(orderList []OrderData) []OrderData {
	now := time.Now().UnixNano()
	f.locker.Lock()
	defer f.locker.Unlock()
	var newOrders []OrderData
	var updatedOrders []OrderData
	for _, existing := range f.orders {
		for _, newOrder := range orderList {
			if existing.Price == newOrder.Price && existing.Amount == newOrder.Amount && existing.Pair == newOrder.Pair {
				existing.Timestamp = now
			} else {
				newOrder.Timestamp = now
				newOrders = append(newOrders, newOrder)
				f.orders = append(f.orders, newOrder)
			}
		}
	}
	for _, old := range f.orders {
		if old.Timestamp >= now-2*int64(time.Hour) {
			updatedOrders = append(updatedOrders, old)
		}
	}
	f.orders = updatedOrders
	return newOrders
}

func ReadJson(resp *http.Response, data interface{}) error {
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid status code: %d", resp.StatusCode)
	}
	bits, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(bits, data)
}

func Now() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func KrwUsd() (float64, error) {
	resp, err := http.Get("https://api.fixer.io/latest?symbols=USD,KRW")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	exc := &ExchangeAnswer{}
	err = ReadJson(resp, exc)
	if err != nil {
		return 0, err
	}
	return exc.Rates.USD / exc.Rates.KRW, nil
}

type ExchangeAnswer struct {
	Rates struct {
		KRW float64 `json:"KRW"`
		USD float64 `json:"USD"`
	} `json:"rates"`
}
