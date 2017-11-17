package crawler

import (
	"fmt"
	"github.com/bitfinexcom/bitfinex-api-go/v2"
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
