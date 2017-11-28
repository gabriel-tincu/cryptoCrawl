package crawler

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
	"net/http"
	"strconv"
	"strings"
	"time"
	"vor-commons/src/github.com/jackc/pgx/pgproto3"
)

var (
	binancePairMapping = map[string]string{
		"BTCUSDT": BTCUSD,
		"ETHUSDT": ETHUSD,
		"ETCUSDT": ETCUSD,
		"BCCUSDT": BCHUSD,
	}
)

const (
	tradeWSSFormat     = "wss://stream.binance.com:9443/ws/%s@aggTrade"
	orderWSSFormat     = "wss://stream.binance.com:9443/ws/%s@depth"
	binanceApiEndpoint = "https://api.binance.com"
)

type BinanceCrawler struct {
	timeDiff  int64
	tradeConn []websocket.Conn
	orderConn []websocket.Conn
	pairs     []string
	writers   []DataWriter
	tradeChan chan TradeMessageBinance
	orderChan chan OrderMessageBinance
}

func NewBinance(writers []DataWriter, pairs []string) (Crawler, error) {
	serverTime, err := getBinanceServerTime()
	if err != nil {
		return nil, err
	}
	timeDiff := serverTime - time.Now().Unix()
	c := &BinanceCrawler{
		pairs:     pairs,
		writers:   writers,
		orderChan: make(chan OrderMessageBinance, 1000),
		tradeChan: make(chan TradeMessageBinance, 1000),
		timeDiff:  timeDiff,
	}
	for _, p := range pairs {
		p = strings.ToLower(p)
		tradeConn, _, err := websocket.DefaultDialer.Dial(fmt.Sprintf(tradeWSSFormat, p), nil)
		if err != nil {
			return nil, err
		}
		c.tradeConn = append(c.tradeConn, *tradeConn)
		orderConn, _, err := websocket.DefaultDialer.Dial(fmt.Sprintf(orderWSSFormat, p), nil)
		if err != nil {
			return nil, err
		}
		c.orderConn = append(c.orderConn, *orderConn)
	}
	return c, nil
}

func (c *BinanceCrawler) produce() {
	for i := range c.orderConn {
		or := c.orderConn[i]
		tr := c.tradeConn[i]
		go c.produceOrder(or)
		go c.produceTrade(tr)
	}
}

func (c *BinanceCrawler) produceOrder(orderChan websocket.Conn) {
	m := &OrderMessageBinance{}
	for {
		err := orderChan.ReadJSON(m)
		if err != nil {
			log.Errorf("error reading from WS: %s", err)
			continue
		}
		c.orderChan <- *m
	}

}

func (c *BinanceCrawler) produceTrade(tradeConn websocket.Conn) {
	m := &TradeMessageBinance{}
	for {
		err := tradeConn.ReadJSON(m)
		if err != nil {
			log.Errorf("error reading from WS: %s", err)
			continue
		}
		c.tradeChan <- *m
	}
}

func (c *BinanceCrawler) Close() {

}

func (c *BinanceCrawler) closeConns() {
	for _, c := range c.orderConn {
		c.Close()
	}
	for _, c := range c.tradeConn {
		c.Close()
	}
}

func (c *BinanceCrawler) Loop() {
	c.produce()
	for {
		select {
		case t, ok := <-c.tradeChan:
			if !ok {
				c.closeConns()
				close(c.orderChan)
				return
			}
			if v, ok := binancePairMapping[t.Pair]; ok {
				typ := buy
				if t.IsMaker {
					typ = sell
				}
				m := TradeMeasurement{
					Amount:          t.Amount,
					Price:           t.Price,
					Pair:            v,
					Platform:        Binance,
					TradeType:       limit,
					Timestamp:       time.Now().Unix(),
					TransactionType: typ,
					Meta:            trade,
				}
				for _, w := range c.writers {
					w.Write(m)
				}
			} else {
				log.Errorf("unrecognized reverse mapping: %s", t.Pair)
			}
		case o, ok := <-c.orderChan:
			if !ok {
				c.closeConns()
				close(c.tradeChan)
				return
			}
			if v, ok := binancePairMapping[o.Pair]; ok {
				for i, b := range o.Bid {
					if b.Amount == 0 {
						continue
					}
					m := OrderMeasurement{
						Pair:      v,
						Meta:      order,
						Timestamp: Now() - int64(i),
						Platform:  Binance,
						Type:      buy,
						Price:     b.Price,
						Amount:    b.Amount,
					}
					for _, w := range c.writers {
						w.Write(m)
					}
				}
				for i, a := range o.Ask {
					if a.Amount == 0 {
						continue
					}
					m := OrderMeasurement{
						Pair:      v,
						Meta:      order,
						Timestamp: Now() - int64(i),
						Platform:  Binance,
						Type:      sell,
						Price:     a.Price,
						Amount:    a.Amount,
					}
					for _, w := range c.writers {
						w.Write(m)
					}
				}
			} else {
				log.Errorf("unrecognized reverse mapping: %s", o.Pair)
			}
		}
	}
}

type TradeMessageBinance struct {
	EventType       string  `json:"e"`
	EventTimestamp  int64   `json:"E"`
	Pair            string  `json:"s"`
	AggregatedTrade int64   `json:"a"`
	Price           float64 `json:"p,string"`
	Amount          float64 `json:"q,string"`
	FirstTradeId    int64   `json:"f"`
	LastTradeId     int64   `json:"l"`
	TradeTimestamp  int64   `json:"T"`
	IsMaker         bool    `json:"m"`
}

type OrderMessageBinance struct {
	EventType string             `json:"e"`
	Timestamp int64              `json:"E"`
	Pair      string             `json:"s"`
	Id        int                `json:"u"`
	Bid       []PricePairBinance `json:"b"`
	Ask       []PricePairBinance `json:"a"`
}

type PricePairBinance struct {
	Amount float64
	Price  float64
}

func (p *PricePairBinance) UnmarshalJSON(data []byte) error {
	var raw []interface{}
	err := json.Unmarshal(data, &raw)
	if err != nil {
		return err
	}
	if len(raw) < 2 {
		return fmt.Errorf("malformed input: %s", string(data))
	}
	if amn, ok := raw[1].(string); ok {
		if am, err := strconv.ParseFloat(amn, 64); err == nil {
			p.Amount = am
		} else {
			return err
		}
	} else {
		return fmt.Errorf("unable to cast %v as amount string", raw[0])
	}
	if pri, ok := raw[0].(string); ok {
		if pr, err := strconv.ParseFloat(pri, 64); err == nil {
			p.Price = pr
		} else {
			return err
		}
	} else {
		return fmt.Errorf("unable to cast %v as price string", raw[0])
	}
	return nil
}

type TimeResponse struct {
	Time int64 `json:"serverTime"`
}

func getBinanceServerTime() (int64, error) {
	r, err := http.Get(binanceApiEndpoint + "/api/v1/time")
	if err != nil {
		return 0, err
	}
	t := &TimeResponse{}
	err = ReadJson(r, t)
	if err != nil {
		return 0, err
	}
	return t.Time, nil
}
