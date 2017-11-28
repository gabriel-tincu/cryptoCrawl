package storage

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"gopkg.in/olivere/elastic.v5"
	"io/ioutil"
	"os"
	"time"
)

const (
	defaultType = "default"
	indexName   = "crypto"
)

type ElasticStorageService struct {
	client       elastic.Client
	ctx          context.Context
	dataChan     chan interface{}
	tickDuration time.Duration
}

func NewESStorage(params map[string]string) (DataWriter, error) {
	log.Debugf("launching new ES storage with params: %+v", params)
	host, ok := params["host"]
	if !ok {
		return nil, fmt.Errorf("param 'host' should be present")
	}
	mappingFile, ok := params["mapping"]
	if !ok {
		return nil, fmt.Errorf("param 'mapping' should be present")
	}
	cli, err := elastic.NewClient(elastic.SetSniff(false), elastic.SetURL(host))
	if err != nil {
		return nil, err
	}

	if fi, err := os.Stat(mappingFile); err != nil {
		return nil, err
	} else {
		if fi.IsDir() {
			return nil, fmt.Errorf("%s points to a directory", mappingFile)
		}
	}
	bits, err := ioutil.ReadFile(mappingFile)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	c := &ElasticStorageService{
		client:       *cli,
		ctx:          ctx,
		dataChan:     make(chan interface{}, 10000),
		tickDuration: parsePeriod(params),
	}
	exists, err := cli.IndexExists(indexName).Do(ctx)
	if exists {
		log.Warnf("index %s already exists, skipping", indexName)
		go c.Ingest()
		return c, nil
	} else {
		log.Debugf("creating new index %s from mapping file %s", indexName, mappingFile)
		_, err = cli.CreateIndex(indexName).Body(string(bits)).Do(ctx)
		if err != nil {
			return nil, err
		} else {
			go c.Ingest()
			return c, nil
		}
	}
}

func (c *ElasticStorageService) Write(data interface{}) {
	c.dataChan <- data
}

func (c *ElasticStorageService) Ingest() {
	var data []interface{}
	tick := time.Tick(c.tickDuration)
	for {
		select {
		case d := <-c.dataChan:
			data = append(data, d)
		case <-tick:
			go c.push(data)
			data = []interface{}{}
		}
	}
}

func (c *ElasticStorageService) push(data []interface{}) {
	if len(data) == 0 {
		return
	}
	var requests []elastic.BulkableRequest
	for _, i := range data {
		requests = append(requests, elastic.NewBulkIndexRequest().Doc(i))
	}
	_, err := c.client.Bulk().Index(indexName).Type(defaultType).Add(requests...).Do(c.ctx)
	if err != nil {
		log.Errorf("error indexing %d docs: %s", len(data), err)
	} else {
		log.Infof("successfully pushed %d bulk datapoints in ES", len(data))
	}
}
