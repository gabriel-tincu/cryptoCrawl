package crawler

import (
	"github.com/beldur/kraken-go-api-client"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

const (
	lastAskTime = "lastAskTime"
	lastBidTime = "lastBidTime"
)

var (
	krakenPairMapping = map[string]string{
		krakenapi.XETHZUSD: ETHUSD,
		krakenapi.XETHZEUR: ETHEUR,
		krakenapi.XXBTZUSD: BTCUSD,
		krakenapi.XXBTZEUR: BTCEUR,
		krakenapi.XETCXUSD: ETCUSD,
		krakenapi.XETCZEUR: ETCEUR,
		krakenapi.XLTCZUSD: LTCUSD,
		krakenapi.XLTCZEUR: LTCEUR,
		krakenapi.XXMRZEUR: XMREUR,
		krakenapi.XXMRZUSD: XMRUSD,
	}
)

type KrakenCrawler struct {
	pairs     []string
	state     sync.Map
	client    krakenapi.KrakenApi
	writers   []DataWriter
	timeDiff  int64
	closeChan chan bool
}

func NewKraken(writers []DataWriter, pairs []string) (Crawler, error) {
	log.Debugf("creating new kraken crawler for pairs %+v and writers %+v", pairs, writers)
	cli := krakenapi.New("", "")
	cl := KrakenCrawler{
		pairs:     pairs,
		client:    *cli,
		writers:   writers,
		state:     sync.Map{},
		closeChan: make(chan bool),
	}
	there, err := cl.client.Time()
	if err != nil {
		panic(err)
	}
	cl.timeDiff = there.Unixtime - time.Now().Unix()
	return &cl, nil
}

func (c *KrakenCrawler) Close() {
	c.closeChan <- true
}

func (c *KrakenCrawler) Loop() {
	for {
		for _, p := range c.pairs {
			select {
			case <- time.After(500 * time.Millisecond):
				go c.ReadTrades(p)
				go c.ReadDepth(p)
			case <- c.closeChan:
				log.Info("closing down kraken crawler")
				return
			}
		}
	}

}

func (c *KrakenCrawler) ReadTrades(symbol string) {
	log.Debugf("reading trade data for pair %s", symbol)
	pairName := krakenPairMapping[symbol]
	if pairName == "" {
		log.Warnf("unable to find mapping for symbol %s", symbol)
		return
	}
	var lastCheck int64
	if lc, ok := c.state.Load(lastTrade + symbol); ok {
		lastCheck = lc.(int64)
	} else {
		log.Warnf("unable to find last trade timestamp")
	}
	trades, err := c.client.Trades(symbol, lastCheck)
	if err != nil {
		log.Errorf("unable to get trade data: %s", err)
		return
	}
	for _, t := range trades.Trades {
		m := TradeMeasurement{
			Meta:      trade,
			Platform:  Kraken,
			Pair:      pairName,
			Amount:    t.VolumeFloat,
			Price:     t.PriceFloat,
			Timestamp: t.Time - c.timeDiff,
		}
		if t.Buy {
			m.TradeType = buy
		} else {
			m.TradeType = sell
		}
		if t.Market {
			m.TransactionType = market
		} else {
			m.TransactionType = limit
		}
	}
	c.state.Store(lastTrade+symbol, trades.Last)
}

func (c *KrakenCrawler) ReadDepth(symbol string) {
	log.Debugf("reading order data for pair %s", symbol)
	pairName := krakenPairMapping[symbol]
	if pairName == "" {
		log.Errorf("unable to find mapping for symbol %s", symbol)
		return
	}
	book, err := c.client.Depth(symbol, 0)
	if err != nil {
		log.Warnf("unable to get order data: %s", err)
		return
	}
	var askTime, bidTime, lastAsk, lastBid int64

	if la, ok := c.state.Load(lastAskTime + symbol); ok {
		askTime = la.(int64)
	} else {
		log.Warnf("unable to find last ask timestamp")
		askTime = 0
	}
	if lb, ok := c.state.Load(lastBidTime + symbol); ok {
		bidTime = lb.(int64)
	} else {
		log.Warnf("unable to find last bid timestamp")
		bidTime = 0
	}
	lastAsk, lastBid = askTime, bidTime
	for _, a := range book.Asks {
		if a.Ts > askTime {
			m := OrderMeasurement{
				Meta:      order,
				Timestamp: a.Ts - c.timeDiff,
				Amount:    a.Amount,
				Price:     a.Price,
				Pair:      pairName,
				Platform:  Kraken,
				Type:      sell,
			}
			for _, w := range c.writers {
				w.Write(m)
			}
			if a.Ts > lastAsk {
				lastAsk = a.Ts
			}
		}
	}
	c.state.Store(lastAskTime+symbol, lastAsk)
	for _, b := range book.Bids {
		if b.Ts > bidTime {
			if b.Ts > askTime {
				m := OrderMeasurement{
					Meta:      order,
					Timestamp: b.Ts - c.timeDiff,
					Amount:    b.Amount,
					Price:     b.Price,
					Pair:      pairName,
					Platform:  Kraken,
					Type:      buy,
				}
				for _, w := range c.writers {
					w.Write(m)
				}
				if b.Ts > lastBid {
					lastBid = b.Ts
				}
			}
		}
	}
	c.state.Store(lastBidTime+symbol, lastBid)
}
