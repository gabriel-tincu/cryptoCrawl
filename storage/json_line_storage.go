package storage

import (
	"encoding/json"
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"time"
)

type JsonLineStorage struct {
	file     os.File
	dataChan chan interface{}
}

func NewJsonLineStorage(params map[string]string) (DataWriter, error) {
	path, ok := params["path"]
	if !ok {
		return nil, fmt.Errorf("parameter 'path' not found in param list: %+v", params)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	writer := &JsonLineStorage{
		file:     *file,
		dataChan: make(chan interface{}, 10000),
	}
	log.Infof("successfully created new JL writer with file %s", path)
	go writer.Ingest()
	return writer, nil
}

func (w *JsonLineStorage) Write(d interface{}) {
	w.dataChan <- d
}

func (w *JsonLineStorage) Ingest() {
	var bData []byte
	ticker := time.NewTicker(10 * time.Second)
	defer w.Close()
	sigChan := make(chan os.Signal, 1)
	for {
		select {
		case l := <-w.dataChan:
			byts, err := json.Marshal(l)
			if err != nil {
				log.Error(err)
				continue
			}
			bData = append(bData, byts...)
			bData = append(bData, '\n')
		case <-ticker.C:
			wr, err := w.file.Write(bData)
			if err != nil {
				log.Error(err)
				continue
			}
			if wr != len(bData) {
				log.Errorf("could not write all data")
				continue
			}
			log.Infof("successfully written %d bytes to %+v", len(bData), w.file)
			bData = []byte{}
		case <-sigChan:
			log.Infof("closing down app")
			break
		}
	}
}

func (w *JsonLineStorage) Close() {
	w.file.Close()
}
