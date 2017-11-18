package storage

import (
	"gopkg.in/olivere/elastic.v5"
	"os"
	"fmt"
	"io/ioutil"
	log "github.com/sirupsen/logrus"
	"context"
	"time"
)

const (
	defaultType = "default"
	indexName = "crypto"
)

type ElasticStorageService struct {
	client elastic.Client
	ctx context.Context
	dataChan chan interface{}
}

func NewESStorage(host string, mappingFile string, dataChan chan interface{}) (*ElasticStorageService, error) {
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
	ctx:= context.Background()
	c := &ElasticStorageService{
		client:*cli,
		ctx:ctx,
		dataChan:dataChan,
	}
	exists, err := cli.IndexExists(indexName).Do(ctx)
	if exists {
		log.Warnf("index %s already exists, skipping", indexName)
		return c, nil
	} else {
		_, err = cli.CreateIndex(indexName).Body(string(bits)).Do(ctx)
		if err != nil {
			return nil, err
		} else {
			return c, nil
		}
	}
}

func (c *ElasticStorageService) Ingest() {
	data := []interface{}{}
	tick := time.Tick(time.Second)
	for {
		select {
		case d := <- c.dataChan:
			data = append(data, d)
		case <- tick:
			go c.push(data)
			data = []interface{}{}
		}
	}
}

func (c *ElasticStorageService) push(data []interface{}) {
	requests := []elastic.BulkableRequest{}
	for _, i := range data {
		requests = append(requests, elastic.NewBulkIndexRequest().Doc(i))
	}
	_, err := c.client.Bulk().Index(indexName).Type(defaultType).Add(requests...).Do(c.ctx)
	if err != nil {
		log.Errorf("error indexing %d docs: %s", len(data), err)
	}
}