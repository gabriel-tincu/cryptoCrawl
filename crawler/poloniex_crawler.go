package crawler

import (
	"encoding/json"
	"fmt"
	"github.com/gammazero/nexus/client"
	"github.com/gammazero/nexus/wamp"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"
)

const (
	poloniex    = "poloniex"
	poloniexURL = "wss://api.poloniex.com"
	modify      = "orderBookModify"
	remove      = "orderBookRemove"
	newTrade    = "newTrade"
)

var (
	poloniexPairMapping = map[string]string{
		"USDT_ETH": ETHUSD,
		"USDT_BTC": BTCUSD,
	}
	typeMapping = map[string]interface{}{
		modify:   &Modify{},
		remove:   &Remove{},
		newTrade: &Trade{},
	}
)

type PoloniexCrawler struct {
	writer   DataWriter
	cli      client.Client
	pairs    []string
	state    sync.Map
	timeDiff int64
}

func NewPoloniex(writer DataWriter, pairs []string) PoloniexCrawler {
	cfg := client.ClientConfig{
		Realm:           "realm1",
		Logger:          log.New(),
		ResponseTimeout: time.Second * 20,
	}
	cli, err := client.ConnectNet(poloniexURL, cfg)
	if err != nil {
		log.Fatalf("error creating wamp client: %s", err)
	}
	log.Infof("created WAMP client")
	return PoloniexCrawler{
		writer:   writer,
		pairs:    pairs,
		cli:      *cli,
		state:    sync.Map{},
		timeDiff: 0,
	}
}

func (c *PoloniexCrawler) Loop() {
	defer c.cli.Close()
	for k, v := range poloniexPairMapping {
		err := c.cli.Subscribe(k, func(args wamp.List, kwargs wamp.Dict, details wamp.Dict) {
			details["pair"] = v
			c.handle(args, kwargs, details)
		}, nil)
		if err != nil {
			log.Fatalf("error subscribing to channel: %s", err)
		}
	}
	log.Infof("all subscriptions done")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	select {
	case <-sigChan:
	case <-c.cli.Done():
		fmt.Println("Router gone, exiting")
		return // router gone, just exit
	}

}

func (c *PoloniexCrawler) handle(args wamp.List, kwargs wamp.Dict, details wamp.Dict) {
	for _, el := range args {
		if tel, ok := el.(map[string]interface{}); ok {
			if dTip, ok := tel[tip].(string); ok {
				var payload interface{}
				if payload, ok = tel[data]; !ok {
					log.Errorf("unable to find data key on payload: %+v", tel)
					continue
				}
				if dt, ok := typeMapping[dTip]; ok {
					bits, err := json.Marshal(payload)
					if err != nil {
						log.Errorf("unable to marshal payload: %s", err)
						continue
					}
					err = json.Unmarshal(bits, dt)
					if err != nil {
						log.Errorf("unable to marshal bytes into %T object: %s", dt, err)
						continue
					}
					err = c.sendData(dt, pair)
					if err != nil {
						log.Errorf("error writing %+v: %s", dt, err)
						continue
					} else {
						log.Debugf("data succesfully written")
					}
				} else {
					log.Errorf("unable to find mapping for type %s", dTip)
					continue
				}
			} else {
				log.Errorf("unable to cast type key to string")
				continue
			}
		} else {
			log.Errorf("unable to cast payload as proper type: %+v", el)
			continue
		}
	}
}

func (c *PoloniexCrawler) sendData(data interface{}, pair string) error {
	switch v := data.(type) {
	case *Modify:
		m := OrderMeasurement{
			Amount:    v.Amount,
			Price:     v.Price,
			Timestamp: time.Now().Unix(),
			Platform:  poloniex,
			Pair:      pair,
			Meta:      cancel,
		}
		if v.Type == bid || v.Type == buy {
			m.Type = buy
		} else if v.Type == ask || v.Type == sell {
			m.Type = sell
		} else {
			return fmt.Errorf("unknown trade type: %s", v.Type)
		}
		return c.writer.Write(m)
	case *Trade:
		// TODO All trades are considered market
		// TODO (natch....but we really need to unify this with the way kraken / others do things)
		// TODO specifically, we need to have SOME way of figuring out, at least as far as past orders are concerned
		// TODO weather they are there to stay or just mud the waters (honor / cancel)
		m := TradeMeasurement{
			Meta:      trade,
			Pair:      pair,
			Platform:  poloniex,
			Price:     v.Price,
			Amount:    v.Amount,
			TradeType: market,
			Timestamp: v.Date.Time.Unix(),
		}
		if v.Type == bid || v.Type == buy {
			m.TransactionType = buy
		} else if v.Type == ask || v.Type == sell {
			m.TransactionType = sell
		} else {
			return fmt.Errorf("unknown trade type: %s", v.Type)
		}
		return c.writer.Write(m)
	case *Remove:
		m := CancelMeasurement{
			Meta:     cancel,
			Price:    v.Price,
			Platform: poloniex,
			Pair:     pair,
			Time:     time.Now().Unix(),
		}
		if v.Type == bid {
			m.Type = buy
		} else if v.Type == ask {
			m.Type = sell
		} else {
			return fmt.Errorf("unknown trade type: %s", v.Type)
		}
		return c.writer.Write(m)
	default:
		return fmt.Errorf("unknown data type: %T", v)
	}
}

type Modify struct {
	Amount float64 `json:"amount,string"`
	Type   string  `json:"type"`
	Price  float64 `json:"rate,string"`
}

func (m *Modify) Unmarshal(data map[string]string) error {
	var am, typ, pric string
	var ok bool
	if am, ok = data[amount]; !ok {
		return fmt.Errorf("error extracting amount")
	}
	if typ, ok = data[tip]; !ok {
		return fmt.Errorf("error extracting type")
	}
	if pric, ok = data[rate]; !ok {
		return fmt.Errorf("error extracting rate")
	}
	if r, err := strconv.ParseFloat(pric, 64); err == nil {
		m.Price = r
	} else {
		return fmt.Errorf("unable to parse rate: %s", err)
	}
	if a, err := strconv.ParseFloat(am, 64); err == nil {
		m.Amount = a
	} else {
		return fmt.Errorf("unable to parse amount: %s", err)
	}
	if typ == bid {
		m.Type = buy
	} else if typ == ask {
		m.Type = sell
	} else {
		fmt.Errorf("unknown event type: %s", typ)
	}
	return nil
}

type Trade struct {
	Price  float64    `json:"rate,string"`
	Type   string     `json:"type"`
	Amount float64    `json:"amount,string"`
	Date   CustomTime `json:"date"`
}

type Remove struct {
	Type  string  `json:"type"`
	Price float64 `json:"rate,string"`
}
