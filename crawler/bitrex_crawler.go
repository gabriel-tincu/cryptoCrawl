package crawler

import (
	log "github.com/sirupsen/logrus"
	"github.com/toorop/go-bittrex"
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
	wsUrl    = "wss://socket.bittrex.com/signalr"
	bitrex = "bitrex"
)

type BitrexCrawler struct {
	writer DataWriter
	client bittrex.Bittrex
	pairs  []string
	data sync.Map
}

func NewBitrex(writer DataWriter, pairs []string) (BitrexCrawler, error) {
	cli := bittrex.New("", "")
	return BitrexCrawler{writer: writer, pairs: pairs, client: *cli, data:sync.Map{}}, nil
}

func (c *BitrexCrawler) Loop() {
	t := time.Tick(500*time.Millisecond)
	for {
		select {
		case <- t:
			for _, p := range c.pairs {
				if v, ok := bitrexPairMapping[p]; ok {
					go func(){
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

func(c *BitrexCrawler) handle(pair string,trades []bittrex.Trade) {
	for _, t := range trades {
		_ = t
	}
}