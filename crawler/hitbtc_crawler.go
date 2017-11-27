package crawler

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"
)

var (
	hitBTCPairMapping = map[string]string{
		ETHUSD: ETHUSD,
		BTCUSD: BTCUSD,
	}
)

const (
	hitBTCUrlBase = "https://api.hitbtc.com/api/2/"
)

type HitBTCCrawler struct {
	pairs   []string
	client  http.Client
	state   sync.Map
	writers []DataWriter
	closeChan chan bool
}

func NewHitBTC(writers []DataWriter, pairs []string) (Crawler, error) {
	cli := http.Client{Timeout: time.Second * 10}
	return &HitBTCCrawler{
		pairs: pairs,
		client: cli,
		state: sync.Map{},
		writers: writers,
		closeChan:make(chan bool)}, nil
}

//unusable due to order of results
func (c *HitBTCCrawler) Orders(pair string) (*HitBTCOrderResponse, error) {
	var lastAskOrder, lastBidOrder HitBTCOrder
	var orders HitBTCOrderResponse
	if la, ok := c.state.Load(lastAskTime + pair); ok {
		if lao, ok := la.(HitBTCOrder); ok {
			lastAskOrder = lao
		}
	}
	if lb, ok := c.state.Load(lastBidTime + pair); ok {
		if lbo, ok := lb.(HitBTCOrder); ok {
			lastBidOrder = lbo
		}
	}

	orderUrl := fmt.Sprintf("%spublic/orderbook/%s", hitBTCUrlBase, strings.ToLower(pair))
	log.Debugf("calling % for orders", orderUrl)
	resp, err := http.Get(orderUrl)
	if err != nil {
		return nil, err
	}
	err = ReadJson(resp, &orders)
	if err != nil {
		return nil, err
	}
	log.Debugf("storing %+v as last ask", orders.Asks[0])
	log.Debugf("storing %+v as last bid", orders.Bids[0])
	c.state.Store(lastAskTime+pair, orders.Asks[0])
	c.state.Store(lastBidTime+pair, orders.Bids[0])
	var i int
	for i = 0; i < len(orders.Asks); i++ {
		log.Debugf("comparing %+v to %+v", lastAskOrder, orders.Asks[i])
		if orders.Asks[i].Amount == lastAskOrder.Amount && orders.Asks[i].Price == lastAskOrder.Price {
			break
		}
	}
	if i < len(orders.Asks) {
		orders.Asks = orders.Asks[:i]
	} else {
		log.Warnf("could not find any stop marker for ask order %+v", lastAskOrder)
	}
	for i = 0; i < len(orders.Bids); i++ {
		log.Debugf("comparing %+v to %+v", lastBidOrder, orders.Bids[i])
		if orders.Bids[i].Amount == lastBidOrder.Amount && orders.Bids[i].Price == lastBidOrder.Price {
			break
		}
	}
	if i < len(orders.Bids) {
		orders.Bids = orders.Bids[:i]
	} else {
		log.Warnf("could not find any stop marker for bid order %+v", lastAskOrder)
	}
	return &orders, nil
}

func (c *HitBTCCrawler) Trades(pair string) ([]HitBTCTradeResponse, error) {
	lastId := -1
	lid, ok := c.state.Load(lastTrade + pair)
	if ok {
		lastId = lid.(int)
	} else {
		log.Warn("could not find last trade id for pair ", pair)
	}
	u, err := url.Parse(hitBTCUrlBase)
	if err != nil {
		return nil, err
	}
	u.Path = path.Join(u.Path, "public", "trades", pair)
	values := url.Values{}
	if lastId != -1 {
		values.Add("from", fmt.Sprintf("%d", lastId))
	}
	u.RawQuery = values.Encode()
	tradeUrl := u.String()
	log.Debugf("calling %s for trades", tradeUrl)
	resp, err := c.client.Get(tradeUrl)
	if err != nil {
		return nil, err
	}
	var decoded []HitBTCTradeResponse
	err = ReadJson(resp, &decoded)
	if err != nil {
		return nil, err
	}
	if len(decoded) > 0 {
		log.Debugf("storing last trade id for pair %s : %d", pair, decoded[0].Id)
		c.state.Store(lastTrade+pair, decoded[0].Id)
		// take out last trade if we've used it as a marker in the method
		if lastId != -1 {
			decoded = decoded[:len(decoded)-1]
		}
	}
	return decoded, nil
}

func (c *HitBTCCrawler) Close() {
	c.closeChan <- true
}

func (c *HitBTCCrawler) Loop() {
	ticker := time.Tick(time.Second * 2)
	for {
		select {
		case <-ticker:
			for _, p := range c.pairs {
				go c.handleTrade(p)
			}
		case <- c.closeChan:
			log.Info("closing down hitbtc crawler")
			return
		}
	}
}

func (c *HitBTCCrawler) handleOrder(pair string) {
	if v, ok := hitBTCPairMapping[pair]; ok {
		orders, err := c.Orders(pair)
		if err != nil {
			log.Errorf("error retrieving hitbtc orders: %s", err)
			return
		}
		for i, a := range orders.Asks {
			m := OrderMeasurement{
				Platform:  HitBTC,
				Meta:      order,
				Type:      buy,
				Pair:      v,
				Price:     a.Price,
				Amount:    a.Amount,
				Timestamp: Now() - int64(i),
			}
			for _, w := range c.writers {
				w.Write(m)
			}
		}
		for i, b := range orders.Bids {
			m := OrderMeasurement{
				Platform:  HitBTC,
				Meta:      order,
				Type:      sell,
				Pair:      v,
				Price:     b.Price,
				Amount:    b.Amount,
				Timestamp: Now() - int64(i),
			}
			for _, w := range c.writers {
				w.Write(m)
			}
		}
	}
}

func (c *HitBTCCrawler) handleTrade(pair string) {
	if v, ok := hitBTCPairMapping[pair]; ok {
		trades, err := c.Trades(pair)
		if err != nil {
			log.Error("error retrieving trades: %s", err)
			return
		}
		for _, response := range trades {
			m := TradeMeasurement{
				Pair:            v,
				Meta:            trade,
				Price:           response.Price,
				Amount:          response.Amount,
				Timestamp:       response.TimeStamp.Unix(),
				Platform:        HitBTC,
				TransactionType: response.Type,
				TradeType:       limit,
			}
			for _, w := range c.writers {
				w.Write(m)
			}
		}

	} else {
		log.Error("unable to find mapping for ", pair)
	}
}

type HitBTCOrder struct {
	Price  float64 `json:"price,string"`
	Amount float64 `json:"size,string"`
}

type HitBTCOrderResponse struct {
	Asks []HitBTCOrder `json:"ask"`
	Bids []HitBTCOrder `json:"bid"`
}

type HitBTCTradeResponse struct {
	Id        int       `json:"id"`
	Price     float64   `json:"price,string"`
	Amount    float64   `json:"quantity,string"`
	Type      string    `json:"side"`
	TimeStamp time.Time `json:"timestamp"`
}
