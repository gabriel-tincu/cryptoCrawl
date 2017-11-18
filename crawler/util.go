package crawler

import (
	"strings"
	"time"
)

type DataWriter interface {
	Write(interface{}) error
}

const (
	ETHUSD = "ETHUSD"
	ETHEUR = "ETHEUR"
	BTCUSD = "BTCUSD"
	BTCEUR = "BTCEUR"

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
	kraken    = "kraken"
)

type CancelMeasurement struct {
	Platform string
	Meta     string
	Type     string
	Pair     string
	Price    float64
	Time     int64
}

type OrderMeasurement struct {
	Meta string
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

type TradeMeasurement struct {
	Meta     string
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

type CrawlerFactory func(writer DataWriter, pairs []string) (Crawler, error)

type Crawler interface {
	Loop()
}

type CrawlerConfig struct {
	Name  string   `json:"name"`
	Pairs []string `json:"pairs"`
}

type Config struct {
	CrawlerCFGS []CrawlerConfig `json:"crawlers"`
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
