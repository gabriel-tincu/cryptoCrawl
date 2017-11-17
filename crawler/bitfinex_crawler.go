package crawler

import (
	"context"
	"fmt"
	"github.com/bitfinexcom/bitfinex-api-go/v2"
	log "github.com/sirupsen/logrus"
	"time"
)

var (
	bitfinexPairMapping = map[string]string{
		bitfinex.BTCUSD: BTCUSD,
		bitfinex.ETHUSD: ETHUSD,
	}
)

const (
	bitfin = "bitfinex"
)

type BitfinexCrawler struct {
	client   bitfinex.Client
	pairs    []string
	timeDiff int64
	writer   DataWriter
}

func NewBitfinex(writer DataWriter, pairs []string) (*BitfinexCrawler, error) {
	cl := bitfinex.NewClient()
	status, err := cl.Platform.Status()
	if !status || err != nil {
		return nil, fmt.Errorf("unable to contact platform")
	}
	crawler := &BitfinexCrawler{
		client:   *cl,
		pairs:    pairs,
		timeDiff: 0,
		writer:   writer,
	}
	return crawler, nil
}

func (c *BitfinexCrawler) Loop() {
	err := c.client.Websocket.Connect()
	if err != nil {
		log.Error(err)
		return
	}
	c.client.Websocket.AttachEventHandler(func(i interface{}) {
		fmt.Println(i)
	})
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
	for {
		select {
		case <-c.client.Websocket.Done():
			log.Info("done processing messages")
			return
		}
	}
}

func (c *BitfinexCrawler) handleTrade(pair string, data interface{}) {
	if fdata, ok := data.([][]float64); ok {
		for _, dpiece := range fdata {
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
				Timestamp:       int64(dpiece[1]),
				Platform:        bitfin,
				Pair:            pair,
				Meta:            trade,
				TransactionType: typ,
				TradeType:       limit,
			}
			c.writer.Write(m)
		}
	} else {
		log.Errorf("unable to convert data: %+v", data)
	}
}

func (c *BitfinexCrawler) handleOrder(pair string, data interface{}) {
	if fdata, ok := data.([][]float64); ok {
		for _, piece := range fdata {
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
				Platform:  bitfin,
				Timestamp: time.Now().Unix(),
				Amount:    amount,
				Price:     price,
				Type:      tip,
			}
			c.writer.Write(m)
		}
	} else {
		log.Errorf("unable to convert data: %+v", data)
	}
}
