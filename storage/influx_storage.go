package storage

import (
	"cryptoCrawl/crawler"
	"fmt"
	"github.com/influxdata/influxdb/client/v2"
	log "github.com/sirupsen/logrus"
	"time"
)

const (
	dbName = "crypto"
)

type InfluxStorageService struct {
	cli         client.Client
	tickDuration time.Duration
	dataChannel chan crawler.InfluxIngestable
}

func NewInfluxStorage(params map[string]string) (DataWriter, error) {
	host, ok := params["host"]
	if !ok {
		return nil, fmt.Errorf("parameter 'host' should be present")
	}
	influxCfg := client.HTTPConfig{
		Timeout: time.Second * 5,
		Addr:    host,
	}
	cli, err := client.NewHTTPClient(influxCfg)
	if err != nil {
		return nil, err
	}
	_, _, err = cli.Ping(time.Second * 5)
	if err != nil {
		return nil, err
	}
	res := &InfluxStorageService{
		cli:         cli,
		dataChannel: make(chan crawler.InfluxIngestable, 10000),
		tickDuration:parsePeriod(params),
	}
	go res.Ingest()
	return res, nil
}

func (c *InfluxStorageService) Write(data interface{}) {
	if ii, ok := data.(crawler.InfluxIngestable); ok {
		c.dataChannel <- ii
	} else {
		log.Errorf("%+v could not be converted to InfluxIngestable", data)
	}
}

func (i *InfluxStorageService) Ingest() {
	var data []crawler.InfluxMeasurement
	ticker := time.Tick(i.tickDuration)
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

func (i *InfluxStorageService) process(data []crawler.InfluxMeasurement) {
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database: dbName,
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
	} else {
		log.Infof("successfully written %d bulk points to influx", len(points))
	}
}
