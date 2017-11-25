package crawler

import (
	log "github.com/sirupsen/logrus"
	"github.com/toorop/go-bittrex"
	"strings"
	"sync"
	"time"
)

var (
	bitrexPairMapping = map[string]string{
		"USDT-ETH": ETHUSD,
		"USDT-BTC": BTCUSD,
	}
)

const (
	wsUrl = "wss://socket.bittrex.com/signalr"
)

type BittrexCrawler struct {
	writers []DataWriter
	client  bittrex.Bittrex
	pairs   []string
	data    sync.Map
	timDiff int64
}

func NewBittrex(writers []DataWriter, pairs []string) (Crawler, error) {
	cli := bittrex.New("", "")
	return &BittrexCrawler{writers: writers, pairs: pairs, client: *cli, data: sync.Map{}}, nil
}

func (c *BittrexCrawler) Loop() {
	t := time.Tick(600 * time.Millisecond)
	for {
		select {
		case <-t:
			for _, p := range c.pairs {
				if v, ok := bitrexPairMapping[p]; ok {
					go func() {
						trades, err := c.client.GetMarketHistory(p)
						if err != nil {
							log.Errorf("error getting market data: %s", err)
						} else {
							c.handle(v, trades)
						}
					}()
				} else {
					log.Errorf("unknown mapping: %s", p)
				}
			}
		}
	}
}

func (c *BittrexCrawler) handle(pair string, trades []bittrex.Trade) {
	if len(trades) == 0 {
		log.Warn("no actual trades to process")
		return
	}
	var lastStoredId int64
	last, ok := c.data.Load(lastTrade + pair)
	c.data.Store(lastTrade+pair, trades[0].OrderUuid)
	if !ok {
		lastStoredId = 0
		log.Warnf("last id not found for pair %s", pair)
	} else {
		lastStoredId = last.(int64)
	}
	for i, t := range trades {
		if lastStoredId == t.OrderUuid {
			break
		}
		m := TradeMeasurement{
			TradeType: limit,
			Meta:      trade,
			Platform:  Bittrex,
			Pair:      pair,
			Timestamp: Now()-int64(i),
			Amount:    t.Quantity,
			Price:     t.Price,
		}
		ttype := strings.ToLower(t.OrderType)
		if ttype != buy && ttype != sell {
			log.Errorf("invalid trade type: %s", t.OrderType)
			return
		}
		m.TransactionType = ttype
		for _, w := range c.writers {
			w.Write(m)
		}
	}
}
