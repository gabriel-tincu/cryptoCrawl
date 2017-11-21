package storage

import (
	"os"
	"fmt"
	"encoding/json"
)

type JsonLineStorage struct {
	file os.File
	dataChan chan interface{}
}

func NewJsonLineStorage (params map[string]string) (DataWriter, error) {
	path, ok := params["path"]
	if !ok {
		return nil, fmt.Errorf("parameret 'path' not found in param list: %+v", params)
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	writer := &JsonLineStorage{
		file:*file,
		dataChan:make(chan interface{}, 10000),
	}
	return writer, nil
}

func (w *JsonLineStorage) Write(d interface{}) {
	w.dataChan <- d
}

func (w *JsonLineStorage) Ingest() {
	defer w.Close()
	for {
		select {
		case l := <- w.dataChan:
			byts, err := json.Marshal(l)
			if err != nil {
				
			}
		}
	}
}

func (w *JsonLineStorage) Close() {
	w.file.Close()
}