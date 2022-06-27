package ckydb

import "time"

type Controller interface {
	Open() error
	Close() error
	Set(key string, value string) error
	Get(key string) (string, error)
	Delete(key string) error
	Clear() error
}

// Connect creates a new Ckydb instance and returns it
func Connect(dbPath string, maxFileSizeKB float64, vacuumIntervalSec float64) (*Ckydb, error) {
	return nil, nil
}

type Ckydb struct {
	tasks       []*time.Ticker
	controlChan chan struct{}
	dbPath      string
}

func (c *Ckydb) Open() error {
	panic("implement me")
}

func (c *Ckydb) Close() error {
	panic("implement me")
}

func (c *Ckydb) Set(key string, value string) error {
	panic("implement me")
}

func (c *Ckydb) Get(key string) (string, error) {
	panic("implement me")
}

func (c *Ckydb) Delete(key string) error {
	panic("implement me")
}

func (c *Ckydb) Clear() error {
	panic("implement me")
}
