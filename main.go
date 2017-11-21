package main

import (
	"cryptoCrawl/crawler"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
)

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

type BasicWriter struct{}

func (b BasicWriter) Write(d interface{}) {
	fmt.Printf("%+v\n", d)
}

type NullWriter struct{}

func (n NullWriter) Write(d interface{}) {}

func getConfig(configFile *string) crawler.Config {
	if configFile == nil || *configFile == "" {
		panic("error reading config file")
	}
	s, err := os.Stat(*configFile)
	if err != nil || s.IsDir() {
		panic("error reading config file")
	}
	cfg := crawler.Config{}
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
	flag.Parse()
	cfg := getConfig(configFile)
	c := cfg.CrawlerCFGS[0]
	_ = c
	cr, err := crawler.NewHitBTC(BasicWriter{}, []string{"ETHUSD", "BTCUSD"})
	if err != nil {
		logrus.Fatal(err)
	}
	cr.Loop()
}
