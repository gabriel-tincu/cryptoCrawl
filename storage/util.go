package storage

import "time"

type DataWriter interface {
	Write(interface{})
}

type WriterFactory = func(params map[string]string) (DataWriter, error)

type WriterConfig struct {
	Name   string            `json:"name"`
	Params map[string]string `json:"params"`
}

func parsePeriod(params map[string]string) (tickPeriod time.Duration) {
	var err error
	period, ok := params["period"]
	if ok {
		tickPeriod, err = time.ParseDuration(period)
		if err != nil {
			tickPeriod = 10 * time.Second
		}
	} else {
		tickPeriod = 10 * time.Second
	}
	return
}
