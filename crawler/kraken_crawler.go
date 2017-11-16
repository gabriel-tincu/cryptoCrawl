package crawler

import (
	"fmt"
	"github.com/beldur/kraken-go-api-client"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

const (
	lastAskTime = "lastAskTime"
	lastTrade   = "lastTrade"
	lastBidTime = "lastAskTime"
)

var (
	krakenPairMapping = map[string]string{
		krakenapi.XETHZUSD: ETHUSD,
		krakenapi.XETHZEUR: ETHEUR,
		krakenapi.XXBTZUSD: BTCUSD,
		krakenapi.XXBTZEUR: BTCEUR,
	}
)

type KrakenCrawler struct {
	pairs    []string
	state    sync.Map
	client   krakenapi.KrakenApi
	writer   DataWriter
	timeDiff int64
}

func NewKraken(writer DataWriter, pairs []string) KrakenCrawler {
	log.Debugf("creating new kraken crawler for pairs %+v", pairs)
	cli := krakenapi.New("", "")
	cl := KrakenCrawler{
		pairs:  pairs,
		client: *cli,
		writer: writer,
		state:  sync.Map{},
	}
	there, err := cl.client.Time()
	if err != nil {
		panic(err)
	}
	cl.timeDiff = there.Unixtime - time.Now().Unix()
	return cl
}

func (c *KrakenCrawler) Loop() {
	tradeTimer := time.NewTicker(500 * time.Millisecond)
	orderTimer := time.NewTicker(500 * time.Millisecond)
	errChan := make(chan error, 10000)
	for {
		select {
		case <-tradeTimer.C:
			for _, p := range c.pairs {
				c.ReadTrades(p, errChan)
			}
		case <-orderTimer.C:
			for _, p := range c.pairs {
				c.ReadDepth(p, errChan)
			}
		case err := <-errChan:
			log.Error(err)
		}
	}

}

func (c *KrakenCrawler) ReadTrades(symbol string, errChan chan error) {
	log.Debugf("reading trade data for pair %s", symbol)
	pairName := krakenPairMapping[symbol]
	if pairName == "" {
		err := fmt.Errorf("unable to find mapping for symbol %s", symbol)
		errChan <- err
		return
	}
	var lastCheck int64
	if lc, ok := c.state.Load(lastTrade + symbol); ok {
		lastCheck = lc.(int64)
	} else {
		errChan <- fmt.Errorf("unable to find last trade timestamp")
		lastCheck = 0
	}
	trades, err := c.client.Trades(symbol, lastCheck)
	if err != nil {
		err = fmt.Errorf("unable to get trade data: %s", err)
		errChan <- err
		return
	}
	for _, t := range trades.Trades {
		m := TradeMeasurement{
			Meta:      trade,
			Platform:  kraken,
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
		err = c.writer.Write(m)
		if err != nil {
			err = fmt.Errorf("error writing data: %s", err)
			errChan <- err
		}
	}
	c.state.Store(lastTrade+symbol, trades.Last)
}

func (c *KrakenCrawler) ReadDepth(symbol string, errChan chan error) {
	log.Debugf("reading order data for pair %s", symbol)
	pairName := krakenPairMapping[symbol]
	if pairName == "" {
		err := fmt.Errorf("unable to find mapping for symbol %s", symbol)
		errChan <- err
		return
	}
	book, err := c.client.Depth(symbol, 0)
	if err != nil {
		err := fmt.Errorf("unable to get order data: %s", err)
		errChan <- err
		return
	}
	var askTime, bidTime, lastAsk, lastBid int64

	if la, ok := c.state.Load(lastAskTime + symbol); ok {
		askTime = la.(int64)
	} else {
		errChan <- fmt.Errorf("unable to find last ask timestamp")
		askTime = 0
	}
	if lb, ok := c.state.Load(lastBidTime + symbol); ok {
		bidTime = lb.(int64)
	} else {
		errChan <- fmt.Errorf("unable to find last bid timestamp")
		bidTime = 0
	}
	lastAsk, lastBid = askTime, bidTime
	for _, a := range book.Asks {
		if a.Ts > askTime {
			data := OrderMeasurement{
				Meta:      order,
				Timestamp: a.Ts - c.timeDiff,
				Amount:    a.Amount,
				Price:     a.Price,
				Pair:      pairName,
				Platform:  kraken,
				Type:      sell,
			}
			err = c.writer.Write(data)
			if err != nil {
				errChan <- err
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
				data := OrderMeasurement{
					Meta:      order,
					Timestamp: b.Ts - c.timeDiff,
					Amount:    b.Amount,
					Price:     b.Price,
					Pair:      pairName,
					Platform:  kraken,
					Type:      buy,
				}
				err = c.writer.Write(data)
				if err != nil {
					errChan <- err
				}
				if b.Ts > lastBid {
					lastBid = b.Ts
				}
			}
		}
	}
	c.state.Store(lastBidTime+symbol, lastBid)
}
