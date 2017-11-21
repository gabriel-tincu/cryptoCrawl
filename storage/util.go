package storage

type DataWriter interface {
	Write(interface{})
}

type WriterFactory = func(params map[string]string) (DataWriter, error)

type WriterConfig struct {
	Name   string            `json:"params"`
	Params map[string]string `json:"params"`
}
