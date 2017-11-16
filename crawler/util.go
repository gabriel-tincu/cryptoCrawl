package crawler

type DataWriter interface {
	Write(interface{}) error
}

const (
	ETHUSD = "ETHUSD"
	ETHEUR = "ETHEUR"
	BTCUSD = "BTCUSD"
	BTCEUR = "BTCEUR"

	market = "marker"
	limit  = "limit"

	buy   = "buy"
	sell  = "sell"
	pair  = "pair"
	trade = "trade"
	order = "order"

	kraken = "kraken"
)

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
