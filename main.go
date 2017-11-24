package main

import (
	"cryptoCrawl/crawler"
	"cryptoCrawl/storage"
	"encoding/json"
	"flag"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
)

func init() {
	log.SetLevel(log.InfoLevel)
}

var (
	crawlerFactories = map[string]crawler.CrawlerFactory{
		crawler.Kraken:   crawler.NewKraken,
		crawler.Poloniex: crawler.NewPoloniex,
		crawler.HitBTC:   crawler.NewHitBTC,
		crawler.Bitstamp: crawler.NewBitStamp,
		crawler.Bitfin:   crawler.NewBitfinex,
		crawler.Bittrex:  crawler.NewBittrex,
		crawler.Binance:  crawler.NewBinance,
	}
	writerFactories = map[string]storage.WriterFactory{
		"elasticsearch": storage.NewESStorage,
		"influxdb":      storage.NewInfluxStorage,
		"jline":         storage.NewJsonLineStorage,
	}
)

type BasicWriter struct{}

func (b BasicWriter) Write(d interface{}) {
	fmt.Printf("%+v\n", d)
}

type Config struct {
	CrawlerCFGS []crawler.CrawlerConfig `json:"crawlers"`
	WriterCFGS  []storage.WriterConfig  `json:"writers"`
}

type NullWriter struct{}

func (n NullWriter) Write(d interface{}) {}

func getConfig(configFile *string) Config {
	if configFile == nil || *configFile == "" {
		panic("error reading config file")
	}
	s, err := os.Stat(*configFile)
	if err != nil || s.IsDir() {
		panic("error reading config file")
	}
	cfg := Config{}
	cfgBytes, err := ioutil.ReadFile(*configFile)
	if err != nil {
		panic("error reading config file")
	}
	err = json.Unmarshal(cfgBytes, &cfg)
	if err != nil {
		panic("error unmarshaling config")
	}
	return cfg
}

func main() {
	configFile := flag.String("config", "config.json", "config file in json format")
	crawlerName := flag.String("crawler", "", "crawler to start")
	flag.Parse()
	if *crawlerName == "" {
		log.Fatalf("crawler name not present")
		return
	}
	mainCfg := getConfig(configFile)
	log.Debugf("working with config %+v", mainCfg)
	if cf, ok := crawlerFactories[*crawlerName]; ok {
		for _, cfg := range mainCfg.CrawlerCFGS {
			if cfg.Name == *crawlerName {
				var writers []crawler.DataWriter
				log.Debugf("parsing writer configs: %+v", mainCfg.WriterCFGS)
				for _, v := range mainCfg.WriterCFGS {
					log.Debugf("searching factory config for %s", v.Name)
					if wrf, ok := writerFactories[v.Name]; ok {
						log.Debugf("adding new writer %s", v.Name)
						dataW, err := wrf(v.Params)
						if err != nil {
							log.Fatalf("error instantiating writer %s: %s", v.Name, err)
						}
						writers = append(writers, dataW)
					}
				}
				crawl, err := cf(writers, cfg.Pairs)
				if err != nil {
					log.Fatalf("error creating crawler with name %s: %s", cfg.Name, err)
				}
				crawl.Loop()
			}
		}
	}
}
