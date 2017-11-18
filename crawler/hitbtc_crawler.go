package crawler

import (
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
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
	hitBTC        = "hitbtc"
	hitBTCUrlBase = "https://api.hitbtc.com/api/2/"
)

type HitBTCCrawler struct {
	pairs  []string
	client http.Client
	state  sync.Map
	writer DataWriter
}

func NewHitBTC(writer DataWriter, pairs []string) (HitBTCCrawler, error) {
	cli := http.Client{Timeout: time.Second * 10}
	return HitBTCCrawler{pairs: pairs, client: cli, state: sync.Map{}, writer: writer}, nil
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
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("invalid status code: %d", resp.StatusCode)
	}
	byts, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	decoded := []HitBTCTradeResponse{}
	err = json.Unmarshal(byts, &decoded)
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

func (c *HitBTCCrawler) Loop() {
	skip := false
	ticker := time.Tick(time.Second * 2)
	for {
		select {
		case <-ticker:
			if skip {
				log.Error("skipping one term due to previous failures")
				skip = false
				continue
			}
			for _, p := range c.pairs {
				go c.handle(p)
			}
		}
	}
}

func (c *HitBTCCrawler) handle(pair string) {
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
				Platform:        hitBTC,
				TransactionType: response.Type,
				TradeType:       limit,
			}
			err := c.writer.Write(m)
			if err != nil {
				log.Error(err)
			}
		}

	} else {
		log.Error("unable to find mapping for ", pair)
	}
}

type HitBTCTradeResponse struct {
	Id        int       `json:"id"`
	Price     float64   `json:"price,string"`
	Amount    float64   `json:"quantity,string"`
	Type      string    `json:"side"`
	TimeStamp time.Time `json:"timestamp"`
}
