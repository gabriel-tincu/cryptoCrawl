package crawler

import "net/http"

var (
	quionePairMapping = map[string]string{
		ETHEUR: ETHEUR,
		BTCEUR: BTCEUR,
		ETHUSD: ETHUSD,
		BTCUSD: BTCUSD,
		BCHUSD: BCHUSD,
	}
)

const (
	quioneProductsUrl = "https://api.quoine.com/products"
)

type QuioneCrawler struct {
	pairsMap  map[string]int
	writers   []DataWriter
	closeChan chan bool
}

func (c *QuioneCrawler) Loop() {
	for {
		for p, id := range c.pairsMap {

		}
	}
}

func (c *QuioneCrawler) Close() {
	c.closeChan <- true
}

func NewQuioneCrawler(writers []DataWriter, pairs []string) (Crawler, error) {
	resp, err := http.Get(quioneProductsUrl)
	if err != nil {
		return nil, err
	}
	var ids []ProductResponse
	defer resp.Body.Close()
	err = ReadJson(resp, &ids)
	if err != nil {
		return nil, err
	}
	pairMapping := map[string]int{}
	for _, p := range ids {
		for _, pair := range pairs {
			if p.Pair == pair {
				pairMapping[pair] = p.ID
			}
		}
	}
	return &QuioneCrawler{pairsMap: pairMapping, writers: writers, closeChan: make(chan bool)}, nil
}

type ProductResponse struct {
	Pair string `json:"currency_pair_code"`
	ID   int    `json:"id,string"`
}
