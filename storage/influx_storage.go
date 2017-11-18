package storage

import (
	"github.com/influxdata/influxdb/client/v2"
	"time"
	"cryptoCrawl/crawler"
	log "github.com/sirupsen/logrus"
)

const (
	dbName = "crypto"
)

type InfluxStorageService struct {
	 cli client.Client
	 dataChannel chan crawler.InfluxIngestable
 }

func NewInfluxStorage(host string, dataChan chan crawler.InfluxIngestable) (*InfluxStorageService, error) {
	influxCfg := client.HTTPConfig{
		Timeout:time.Second*5,
		Addr:host,
	}
	cli, err := client.NewHTTPClient(influxCfg)
	if err != nil {
		return nil, err
	}
	_, _, err = cli.Ping(time.Second*5)
	if err != nil {
		return nil, err
	}
	return &InfluxStorageService{
		cli:cli,
		dataChannel: dataChan,
	}, nil
}

func (i *InfluxStorageService) Ingest() {
	var data []crawler.InfluxMeasurement
	ticker := time.Tick(time.Second * 2)
	for {
		select {
		case d := <-i.dataChannel:
			data = append(data, d.AsInfluxMeasurement())
		case <-ticker:
			go i.process(data)
			data = []crawler.InfluxMeasurement{}
		}
	}
}

func(i *InfluxStorageService) process(data []crawler.InfluxMeasurement) {
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:dbName,
	})
	if err != nil {
		log.Error(err)
		return
	}
	var points []*client.Point
	for _, d := range data {
		p, err := client.NewPoint(d.Measurement, d.Tags, d.Fields, d.Timestamp)
		if err != nil {
			log.Errorf("error making a point out of %+v: %s", d, err)
			continue
		}
		points = append(points, p)
	}
	bp.AddPoints(points)
	err = i.cli.Write(bp)
	if err != nil {
		log.Errorf("error writing %d points to influx: %s", len(points), err)
	}
}