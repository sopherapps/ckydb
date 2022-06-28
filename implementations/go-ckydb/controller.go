package ckydb

import (
	"log"
	"time"

	"github.com/sopherapps/ckydb/implementations/go-ckydb/internal"
)

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
	db, err := NewCkydb(dbPath, maxFileSizeKB, vacuumIntervalSec)
	if err != nil {
		return nil, err
	}

	err = db.Open()
	if err != nil {
		return nil, err
	}

	return db, nil
}

type Ckydb struct {
	tasks             []internal.Worker
	store             internal.Storage
	vacuumIntervalSec float64
}

func NewCkydb(dbPath string, maxFileSizeKB float64, vacuumIntervalSec float64) (*Ckydb, error) {
	store := internal.NewStore(dbPath, maxFileSizeKB)
	err := store.Load()
	if err != nil {
		return nil, err
	}

	db := Ckydb{
		tasks:             make([]internal.Worker, 0),
		store:             store,
		vacuumIntervalSec: vacuumIntervalSec,
	}

	return &db, nil
}

func (c *Ckydb) Open() error {
	vacuumTask := internal.NewTask(time.Second*time.Duration(c.vacuumIntervalSec), func() {
		err := c.store.Vacuum()
		if err != nil {
			log.Printf("error: %s", err)
		}
	})

	c.tasks = append(c.tasks, vacuumTask)

	return nil
}

func (c *Ckydb) Close() error {
	for _, task := range c.tasks {
		err := task.Stop()
		if err != nil {
			return err
		}
	}

	c.tasks = nil
	return nil
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
