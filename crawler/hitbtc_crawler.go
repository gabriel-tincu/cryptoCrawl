package crawler

import (
	"net/http"
	"sync"
	"time"
)

var (
	hitBTCPairMapping = map[string]string {
		ETHUSD: ETHUSD,
		BTCUSD: BTCUSD,
	}
)

const (
	hitBTCUrlBase = "https://api.hitbtc.com/api/2/"
)

type HitBTCCrawler struct {
	pairs []string
	client http.Client
	state sync.Map
	writer DataWriter
}

func NewHitBTC(writer DataWriter, pairs []string)(HitBTCCrawler, error) {
	cli := http.Client{Timeout:time.Second*10}
	return HitBTCCrawler{pairs:pairs, client:cli, state:sync.Map{}, writer:writer}, nil
}

func (c *HitBTCCrawler) Loop() {

}