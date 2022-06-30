package ckydb

import (
	"log"
	"sync"
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

type Ckydb struct {
	tasks             []internal.Worker
	store             internal.Storage
	vacuumIntervalSec float64
	isOpen            bool
	lock              sync.Mutex
}

// Connect creates a new Ckydb instance, starts its background tasks and returns it
func Connect(dbPath string, maxFileSizeKB float64, vacuumIntervalSec float64) (*Ckydb, error) {
	db, err := newCkydb(dbPath, maxFileSizeKB, vacuumIntervalSec)
	if err != nil {
		return nil, err
	}

	err = db.Open()
	if err != nil {
		return nil, err
	}

	return db, nil
}

// newCkydb creates a new instance of Ckydb. This is used internally.
// Use Connect() for external code
func newCkydb(dbPath string, maxFileSizeKB float64, vacuumIntervalSec float64) (*Ckydb, error) {
	store := internal.NewStore(dbPath, maxFileSizeKB)
	err := store.Load()
	if err != nil {
		return nil, err
	}

	db := Ckydb{
		tasks:             make([]internal.Worker, 0),
		store:             store,
		vacuumIntervalSec: vacuumIntervalSec,
		isOpen:            false,
	}

	return &db, nil
}

// Open initializes all background tasks
func (c *Ckydb) Open() error {
	if c.isOpen {
		return nil
	}

	vacuumTask := internal.NewTask(time.Second*time.Duration(c.vacuumIntervalSec), func() {
		c.lock.Lock()
		defer c.lock.Unlock()

		err := c.store.Vacuum()
		if err != nil {
			log.Printf("error: %s", err)
		}
	})
	err := vacuumTask.Start()
	if err != nil {
		return err
	}

	c.tasks = append(c.tasks, vacuumTask)
	c.isOpen = true

	return nil
}

// Close stops any background tasks
func (c *Ckydb) Close() error {
	if !c.isOpen {
		return nil
	}

	for _, task := range c.tasks {
		err := task.Stop()
		if err != nil {
			return err
		}
	}

	c.isOpen = false
	return nil
}

// Set adds or updates the value corresponding to the given key in store
// It might return an ErrCorruptedData error but if it succeeds, no error is returned
func (c *Ckydb) Set(key string, value string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.store.Set(key, value)
}

// Get retrieves the value corresponding to the given key
// It returns a ErrNotFound error if the key is nonexistent
func (c *Ckydb) Get(key string) (string, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.store.Get(key)
}

// Delete removes the key-value pair corresponding to the passed key
// It returns an ErrNotFound error if the key is nonexistent
func (c *Ckydb) Delete(key string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.store.Delete(key)
}

// Clear resets the entire Store, and clears everything on disk
func (c *Ckydb) Clear() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.store.Clear()
}
