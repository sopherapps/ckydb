package main

import (
	"errors"
	"fmt"
	"github.com/sopherapps/ckydb/implementations/go-ckydb"
	"log"
	"path/filepath"
)

func main() {
	records := map[string]string{
		"hey":      "English",
		"hi":       "English",
		"salut":    "French",
		"bonjour":  "French",
		"hola":     "Spanish",
		"oi":       "Portuguese",
		"mulimuta": "Runyoro",
	}

	dbPath, err := filepath.Abs("db")
	if err != nil {
		log.Fatal("error getting db path ", err)
	}

	db, err := ckydb.Connect(dbPath, 2, 300)
	if err != nil {
		log.Fatal("error connecting to db ", err)
	}
	defer func() { _ = db.Close() }()

	// setting the keys
	for k, v := range records {
		err = db.Set(k, v)
		if err != nil {
			log.Fatal("error setting keys ", err)
		}
	}

	fmt.Println("\n\nAfter setting keys")
	fmt.Println("====================")
	for k := range records {
		v, err := db.Get(k)
		if err != nil {
			log.Fatal("error getting values ", err)
		}

		fmt.Printf("key: %s, value: %s\n", k, v)
	}

	// updating keys
	newValues := map[string]string{
		"hey":      "Jane",
		"hi":       "John",
		"hola":     "Santos",
		"oi":       "Ronaldo",
		"mulimuta": "Aliguma",
	}
	for k, v := range newValues {
		err = db.Set(k, v)
		if err != nil {
			log.Fatal("error updated keys ", err)
		}
	}

	fmt.Println("\n\nAfter updating keys")
	fmt.Println("=====================")
	for k := range records {
		v, err := db.Get(k)
		if err != nil {
			log.Fatal("error getting values ", err)
		}

		fmt.Printf("key: %s, value: %s\n", k, v)
	}

	// deleting the keys
	keysToDelete := []string{"oi", "hi"}
	for _, key := range keysToDelete {
		err = db.Delete(key)
		if err != nil {
			log.Fatal("error deleting keys ", err)
		}
	}

	fmt.Printf("\n\nAfter deleting keys %v\n", keysToDelete)
	fmt.Println("=============================")
	for k := range records {
		v, err := db.Get(k)
		if err != nil {
			if errors.Is(err, ckydb.ErrNotFound) {
				fmt.Printf("deleted key: %s, error: %s\n", k, err)
			} else {
				log.Fatal("error getting values ", err)
			}
		} else {
			fmt.Printf("key: %s, value: %s\n", k, v)
		}
	}

	// clear the database
	err = db.Clear()
	if err != nil {
		log.Fatal("error clearing db ", err)
	}

	fmt.Println("\n\nAfter clearing")
	fmt.Println("=================")
	for k := range records {
		v, err := db.Get(k)
		if err == nil {
			log.Fatalf("ErrNotFound not returned for Key: %s, value: %s", k, v)
		}

		fmt.Printf("deleted key: %s, error: %s\n", k, err)
	}
}
