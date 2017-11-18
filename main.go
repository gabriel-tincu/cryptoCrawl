package main

import (
	"cryptoCrawl/crawler"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
)

type BasicWriter struct{}

func (b BasicWriter) Write(d interface{}) error {
	fmt.Printf("%+v\n", d)
	return nil
}

type NullWriter struct{}

func (n NullWriter) Write(d interface{}) error { return nil }

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
	pol, err := crawler.NewHitBTC(BasicWriter{}, []string{"BTCUSD"})
	if err != nil {
		panic(err)
	}
	pol.Loop()
}
