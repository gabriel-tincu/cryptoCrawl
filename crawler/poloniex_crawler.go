package crawler

import (
	"fmt"
	"github.com/gammazero/nexus/client"
	"github.com/gammazero/nexus/wamp"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"sync"
	"time"
)

var (
	poloniexPairMapping = map[string]string{
		"USDT_ETH": ETHUSD,
		"USDT_BTC": BTCUSD,
	}
)

const (
	poloniexURL = "wss://api.poloniex.com"
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
	log.Infof("allsubscriptions done")
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
	written := false
	for _, el := range args {
		tel := el.(map[string]interface{})
		if tel["type"] != "orderBookRemove" {
			if !written {
				fmt.Println(details["pair"])
				written = true
			}
			fmt.Println(tel)
		}
	}
	if !written {
		log.Debug("no data of interest received")
	}
}
