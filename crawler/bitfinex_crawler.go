package crawler

import (
	"context"
	"fmt"
	"github.com/bitfinexcom/bitfinex-api-go/v2"
	log "github.com/sirupsen/logrus"
	"os"
)

var (
	bitfinexPairMapping = map[string]string{
		bitfinex.BTCUSD: BTCUSD,
		bitfinex.ETHUSD: ETHUSD,
	}
)

type BitfinexCrawler struct {
	client   bitfinex.Client
	pairs    []string
	timeDiff int64
	writers  []DataWriter
	closeChan chan bool
}

func NewBitfinex(writers []DataWriter, pairs []string) (Crawler, error) {
	cl := bitfinex.NewClient()
	status, err := cl.Platform.Status()
	if !status || err != nil {
		return nil, fmt.Errorf("unable to contact platform")
	}
	crawler := &BitfinexCrawler{
		client:   *cl,
		pairs:    pairs,
		timeDiff: 0,
		writers:  writers,
		closeChan: make(chan bool),
	}
	crawler.connect()
	return crawler, nil
}

func (c *BitfinexCrawler) Close() {
	c.closeChan <- true
}

func (c *BitfinexCrawler) connect() {
	err := c.client.Websocket.Connect()
	if err != nil {
		log.Error(err)
		return
	}
	ctx := context.Background()
	for _, p := range c.pairs {
		var v string
		var ok bool
		if v, ok = bitfinexPairMapping[p]; !ok {
			log.Errorf("unable to find mapping for %s", p)
			continue
		}
		msg := &bitfinex.PublicSubscriptionRequest{
			Pair:    p,
			Channel: bitfinex.ChanBook,
			Event:   "subscribe",
		}
		handler := func(data interface{}) {
			c.handleOrder(v, data)
		}
		err = c.client.Websocket.Subscribe(ctx, msg, handler)
		if err != nil {
			log.Error(err)
			return
		}
		msg = &bitfinex.PublicSubscriptionRequest{
			Pair:    p,
			Channel: bitfinex.ChanTrades,
			Event:   "subscribe",
		}
		handler = func(data interface{}) {
			c.handleTrade(v, data)
		}
		err = c.client.Websocket.Subscribe(ctx, msg, handler)
		if err != nil {
			log.Fatal(err)
			return
		}
	}
}

func (c *BitfinexCrawler) Loop() {
	for {
		select {
		case <-c.client.Websocket.Done():
			log.Info("client disconnected, reconnecting")
			c.connect()
		case <- c.closeChan:
			log.Info("closing down bitfinex client")
			c.client.Websocket.Close()
			return
		}
	}
}

func (c *BitfinexCrawler) handleTrade(pair string, data interface{}) {
	if _, ok := data.(bitfinex.Heartbeat); ok {
		return
	}
	if fdata, ok := data.([][]float64); ok {
		for i, dpiece := range fdata {
			if len(dpiece) != 4 {
				log.Errorf("malformed input for trade: %+v", dpiece)
			}
			total := dpiece[2]
			typ := buy
			if total < 0 {
				total = -1 * total
				typ = sell
			}
			m := TradeMeasurement{
				Price:           dpiece[3],
				Amount:          total,
				Timestamp:       Now()-int64(i),
				Platform:        Bitfin,
				Pair:            pair,
				Meta:            trade,
				TransactionType: typ,
				TradeType:       limit,
			}
			for _, w := range c.writers {
				w.Write(m)
			}
		}
	} else {
		log.Errorf("unable to convert data: %+v: %T", data, data)
	}
}

func (c *BitfinexCrawler) handleOrder(pair string, data interface{}) {
	if _, ok := data.(bitfinex.Heartbeat); ok {
		return
	}
	if fdata, ok := data.([][]float64); ok {
		for i, piece := range fdata {
			if len(piece) != 3 {
				log.Errorf("malformed input: %+v", piece)
				continue
			}
			price := piece[0]
			amount := piece[2]
			tip := buy
			if amount < 0 {
				tip = sell
				amount = -1 * amount
			}
			m := OrderMeasurement{
				Meta:      order,
				Pair:      pair,
				Platform:  Bitfin,
				Timestamp: Now()-int64(i),
				Amount:    amount,
				Price:     price,
				Type:      tip,
			}
			for _, w := range c.writers {
				w.Write(m)
			}
		}
	} else {
		log.Errorf("unable to convert data: %+v", data)
	}
}
