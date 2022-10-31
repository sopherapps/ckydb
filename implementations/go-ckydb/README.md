# ckydb

A simple fast memory-first thread-safe (or goroutine-safe for Go) key-value embedded database that persists data on disk.

It is read as 'skydb' This the Golang implementation of ckydb

## Quick Start

- Create a new Go modules project

```shell
mkdir ckydb_example
go mod init ckydb_example
```

- Install ckydb

```shell
go get github.com/sopherapps/ckydb/implementations/go-ckydb
```

- Create a main.go file in the project folder and add the following code

```go
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
```

- Run the `main.go` module and observe the terminal

```shell
go run main.go
```

## How to Run Tests

- Clone the repo

```shell
git clone git@github.com:sopherapps/ckydb.git
```

- Enter the go implementation folder

```shell
cd ckydb/implementations/go-ckydb
```

- Install dependencies

```shell
go mod tidy
```
- Run the test command

```shell
go test ./...
```

- Run the benchmark tests

```shell
go test -bench=. -run=^#
```

- Latest benchmarks are as shown below:

```shell
goos: darwin
goarch: amd64
pkg: github.com/sopherapps/ckydb/implementations/go-ckydb
cpu: Intel(R) Core(TM) i7-5557U CPU @ 3.10GHz
BenchmarkCkydb/Set_hey_English-4                    7128            155768 ns/op
BenchmarkCkydb/Set_hi_English-4                     8040            152184 ns/op
BenchmarkCkydb/Set_salut_French-4                   6781            148910 ns/op
BenchmarkCkydb/Set_bonjour_French-4                 8011            138914 ns/op
BenchmarkCkydb/Set_hola_Spanish-4                   8264            144506 ns/op
BenchmarkCkydb/Set_oi_Portuguese-4                  8353            148823 ns/op
BenchmarkCkydb/Set_mulimuta_Runyoro-4               8340            147906 ns/op
BenchmarkCkydb/Get_hi-4                         21843999                51.76 ns/op
BenchmarkCkydb/Get_salut-4                      22164397                51.74 ns/op
BenchmarkCkydb/Get_bonjour-4                    24162084                45.64 ns/op
BenchmarkCkydb/Get_hola-4                       39530481                28.52 ns/op
BenchmarkCkydb/Get_oi-4                         36606564                29.52 ns/op
BenchmarkCkydb/Get_mulimuta-4                   34984358                31.45 ns/op
BenchmarkCkydb/Get_hey-4                        23291575                51.16 ns/op
BenchmarkCkydb/Update_mulimuta_Aliguma-4            7328            166744 ns/op
BenchmarkCkydb/Update_hey_Jane-4                    7428            151814 ns/op
BenchmarkCkydb/Update_hi_John-4                     7833            151954 ns/op
BenchmarkCkydb/Update_salut_Jean-4                  7737            152974 ns/op
BenchmarkCkydb/Update_oi_Ronaldo-4                  7584            150535 ns/op
BenchmarkCkydb/Delete_hi-4                      35498058                32.08 ns/op
BenchmarkCkydb/Delete_salut-4                   35957875                31.59 ns/op
BenchmarkCkydb/Delete_oi-4                      35782756                34.47 ns/op
BenchmarkCkydb/Delete_mulimuta-4                34095344                31.71 ns/op
BenchmarkCkydb/Delete_hey-4                     32985225                32.33 ns/op
BenchmarkCkydb/Clear-4                              1532            729148 ns/op
PASS
ok      github.com/sopherapps/ckydb/implementations/go-ckydb    35.500s
```

## Under the Hood

- Every key has a TIMESTAMP key, added to it on creation. This TIMESTAMPED key is the one used to store data in a
  sorted way for easy retrieval.
- The actual key known by user, however, is kept in the index. When ckydb is initialized, the index is loaded into
  memory from the index file (a ".idx" file). The index is basically a map of `key: TIMESTAMPED-key`
- The TIMESTAMPED-key and its value are stored first in a log file (a ".log" file). This current log file has an
  in-memory copy we call `memtable`
- When the current log file exceeds a predefined size `maxFileSizeKB`, it is converted to a data file (a ".cky"
  file) and `memtable` refreshed and a new log file created.
- The names of each ".cky" or ".log" file are the timestamps when they were created. Do note that conversion of ".log"
  to "cky" just changes the file extension.
- There is always one ".log" file in the database folder. If on initialization, there is no ".log" file, a new one is
  created.
- There is an in-memory sorted list of ".cky" files called `data_files` that is kept keysToDelete everytime a ".log" file is
  converted into ".cky".
- The name of the current log (`currentLogFile`) file is also kept in memory.
- There is also a ".del" file that holds all the `key: TIMESTAMPED-key` pairs that have been marked for deletion.
- At a predefined interval `vacuumIntervalSec`, a background task deletes the values from ".cky" and ".log" files
  corresponding to the `key: TIMESTAMPED-key` pairs found in the ".del" file. Each deleted pair is then removed from
  the ".del" file.
- On initial load, any keys in .del should have their values deleted in the corresponding ".log" or ".cky" files

### Operations

- On `db.Set(key, value)`:
    - the corresponding TIMESTAMPED key is searched for in the index
    - if the key does not exist:
        - a new TIMESTAMPED key is created and added to the index with its user-defined key
        - the user-defined key and its TIMESTAMPED key are then added to the index file (".idx")
        - this TIMESTAMPED key and its value are then added to `memtable`.
        - this TIMESTAMPED key and its value are then added to the current log file (".log")
        - A check is made on the size of the log file. If the log file is bigger than the max size allowed,
          it is rolled into a .cky file and a new log file created, and the `memtable` refreshed.
    - if the key exists:
        - its timestamp is extracted and compared to the current_log file to see if it is later than the current_log
          file
        - if it is later or equal, `memtable` and the current log file are keysToDelete
        - else the timestamp is compared to cache's "start" and "stop" to see if it lies within the cache
        - if it exists in the cache, then the cache data and its corresponding data file are keysToDelete
        - else, the data file in which the timestamp exists is located within the data_files. This is done by finding
          the two data files between which the timestamp exists when the list is sorted in ascending order. The file to
          the left is the one containing the timestamp.
            - the key-values from the data file are then extracted and they new key-value inserted
            - the new data is then loaded into the cache
            - the new data is also loaded into the data file
    - If any error occurs on any of these steps, the preceding steps are reversed and the error returned
      in the call

- On `db.Delete(key)`:
    - Its `key: TIMESTAMPED-key` pair is removed from the in-memory index.
    - Its `key: TIMESTAMPED-key` pair is removed from the ".idx" file
    - Its `key: TIMESTAMPED-key` is added to the ".del" file
    - If any error occurs on any of these steps, the preceding steps are reversed and the error returned
      in the call

- On `db.Get(key)`:
    - the corresponding TIMESTAMPED key is searched for in the index
    - if the key does not exist, an ErrNotFound error is returned.
    - if the key exists, its TIMESTAMP is extracted and checked if it is greater (later) than the name of the current
      log file.
    - if this TIMESTAMP is later, its value is quickly got from `memtable` in memory. If for some crazy reason, it does
      not exist there, an ErrCorruptedData error is returned.
    - If this TIMESTAMP is earlier than the name of the current log file, the TIMESTAMP is compared to the range in the
      memory `cache`, if it falls there in, its value is got from `cache`. If the value is not found for some reason, a
      ErrCorruptedData error is returned
    - Otherwise the ".cky" file whose name is earlier than the TIMESTAMP but whose neighbour to the right, in the
      in-memory sorted `dataFiles` list, is later than TIMESTAMP is loaded into an in-memory `cache` whose range is set
      to two ".cky" filenames between which it falls.
    - the value is then got from `cache`'s data. If it is not found for some reason, an ErrCorruptedData is
      returned

- On `db.Clear()`:
    - `memtable` is reset
    - `cache` is reset
    - `index` in memory is reset
    - `data_files` in memory is reset
    - all files in the database folder are deleted
    - A new ".log" file is created

### File formats

- The file format of the ".idx" index files is just "key<key_value_separator>TIMESTAMPED-key<token>" separated by a
  unique token e.g. "{&*/%}" and a key_value_separator e.g. "[><?&(^#]"

```
goat[><?&(^#]1655304770518678-goat{&*/%}hen[><?&(^#]1655304670510698-hen{&*/%}pig[><?&(^#]1655304770534578-pig{&*/%}fish[><?&(^#]1655303775538278-fish$%#@*&^&
```

- The file format of the ".del" files is just "TIMESTAMPED-key<token>" separated by a unique token e.g. "{&*/%}"

```
1655304770518678-goat{&*/%}1655304670510698-hen{&*/%}1655304770534578-pig{&*/%}1655303775538278-fish$%#@*&^&
```

- The file format of the ".log" and ".cky" files is just  "TIMESTAMPED-key<key_value_separator>value<token>" separated
  by a unique token e.g. "{&*/%}" and a key_value_separator like "[><?&(^#]"

```
1655304770518678-goat[><?&(^#]678 months{&*/%}1655304670510698-hen[><?&(^#]567 months{&*/%}1655304770534578-pig[><?&(^#]70 months{&*/%}1655303775538278-fish[><?&(^#]8990 months$%#@*&^&
```

## Ideas For Improvement

- [ ] Explicitly allow for multiple concurrent reads (e.g. don't lock at all on read)
- [ ] Explicitly allow for conditional multiple concurrent writes (e.g. lock on key, not on store)
- [ ] Distribute the database across different machines or nodes (
    e.g. have multiple backend nodes, and let each node's timestamped key range be recorded on the
     master/main/gateway node(s). The gateway nodes themselves could be replicated. Clients read/update
     data through the gateway node)

### Multiple Concurrent Reads, Single Writes at a time

- Have no lock on the main routine of `ckydb.Get`.
  `ckydb.Get` has props `index`, `memtable` and `cache` as its source of truth.
- To avoid using a stale `cache` and yet also avoid data races between `store.Set` and `store.Get`, both,
  of old keys, we have a `cacheLock` lock.
  This lock is to be obtained by either `store.Get` or `store.Set` both for old keys
- Have the same `mutLock` lock on the `ckydb.Delete` and `ckydb.Set`.
  If you had separate locks, there would be chance for a data race.
- For `ckydb.Clear`, update `index` **first**.
- For `ckydb.Delete`, update `index` **last**.
- For `ckydb.Set` of a new key (i.e. not an update), update `index` **last**.
- For `ckydb.Set` of pre-existing key, update `memtable` or `cache` **last** as index would already be up-to-date.
- For `store.vacuum` task and `store.Delete`, there will be a `delFileLock` within store to avoid conflicts.


## Acknowledgments

- We can do nothing without God (John 15: 5). Glory be to Him.
- Some of these ideas were adapted from [leveldb](https://github.com/google/leveldb). Thanks.

## License

Copyright (c) 2022 [Martin Ahindura](https://github.com/tinitto). All implementations are licensed under
the [MIT License](./LICENSE)
