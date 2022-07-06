# ckydb

A simple fast memory-first thread-safe key-value embedded database that persists data on disk.

It is read as 'skydb' This the rust implementation of ckydb

## Quick Start

- Create a new project and activate your virtual environment

```shell
cargo new ckydb_example
```

- Add ckydb to the `Cargo.toml` as a dependency

```TOML
[dependencies]
ckydb = { version = "0.0.5" } # put the appropriate version. 0.1.0 is just an example
```

- Add the following code to the `src/main.rs` file

```rust
use ckydb::{connect, Controller};

fn main() {
    let mut db = connect("db", 4.0, 60.0).unwrap();
    let keys = ["hey", "hi", "yoo-hoo", "bonjour"].to_vec();
    let values = ["English", "English", "Slang", "French"].to_vec();

    // Setting the values
    println!("[Inserting key-value pairs]");
    for (k, v) in keys.clone().into_iter().zip(values) {
        let _ = db.set(k, v);
    }

    // Getting the values
    println!("[After insert]");
    for k in keys.clone() {
        let got = db.get(k).unwrap();
        println!("For key: {:?}, Got: {:?}", k, got);
    }

    // Deleting some values
    for k in &keys[2..] {
        let removed = db.delete(*k);
        println!("Removed: key: {:?}, resp: {:?}", k, removed);
    }

    for k in &keys {
        let got = db.get(*k);
        println!("[After delete: For key: {:?}, Got: {:?}", k, got);
    }

    // Deleting all values
    let cleared = db.clear();
    println!("Cleared: {:?}", cleared);

    println!("[After clear]");
    for k in &keys {
        let got = db.get(*k);
        println!("For key: {:?}, Got: {:?}", k, got);
    }
    db.close().expect("close");
}

```

- Run the app and observe the terminal

```shell
cargo run
```

## Examples

Some examples can be found in the /examples folder.

```shell
cargo run --example hello_ckydb
```

## How to Run Tests

- Clone the repo

```shell
git clone git@github.com:sopherapps/ckydb.git
```

- Enter the rust implementation folder

```shell
cd ckydb/implementations/rs_ckydb
```

- Run the test command

```shell
cargo test
```

- Run the bench test command

```shell
cargo bench
```

- The latest benchmarking results are:

```shell
set hey English         time:   [137.62 us 141.90 us 147.99 us]                            
set hi English          time:   [192.54 us 225.35 us 261.72 us]                           
set salut French        time:   [132.90 us 135.63 us 139.30 us]                             
set bonjour French      time:   [132.69 us 133.80 us 135.11 us]                               
set hola Spanish        time:   [134.12 us 135.83 us 138.13 us]                             
set oi Portuguese       time:   [134.64 us 136.19 us 138.08 us]                              
set mulimuta Runyoro    time:   [136.19 us 137.73 us 139.53 us]                                 
update hey to Jane      time:   [136.23 us 138.95 us 143.04 us]                               
update hi to John       time:   [135.33 us 136.72 us 138.30 us]                              
update hola to Santos   time:   [135.17 us 136.60 us 138.29 us]                                  
update oi to Ronaldo    time:   [135.23 us 137.36 us 140.53 us]                                 
update mulimuta to Aliguma                                                                            
                        time:   [136.02 us 137.23 us 138.61 us]
get hey                 time:   [300.57 ns 302.71 ns 305.10 ns]                    
get hi                  time:   [365.49 ns 425.81 ns 501.96 ns]                   
get salut               time:   [295.12 ns 296.45 ns 299.09 ns]                      
get bonjour             time:   [311.02 ns 325.51 ns 346.03 ns]                        
get hola                time:   [295.52 ns 295.83 ns 296.21 ns]                     
get oi                  time:   [288.47 ns 289.03 ns 289.69 ns]                   
get mulimuta            time:   [296.09 ns 296.30 ns 296.55 ns]                         
delete hey              time:   [44.772 ns 53.713 ns 65.174 ns]                        
delete hi               time:   [39.170 ns 41.810 ns 45.560 ns]                       
delete salut            time:   [40.376 ns 41.609 ns 43.097 ns]                          
delete bonjour          time:   [43.702 ns 44.510 ns 45.426 ns]                            
delete hola             time:   [38.753 ns 39.253 ns 39.788 ns]                         
delete oi               time:   [39.040 ns 39.266 ns 39.552 ns]                       
delete mulimuta         time:   [23.381 ns 23.572 ns 23.771 ns]                             
clear                   time:   [573.03 us 596.06 us 620.95 us]                  
```

## Under the Hood

- Every key has a TIMESTAMP prefix, added to it on creation. This TIMESTAMPED key is the one used to store data in a
  sorted way for easy retrieval.
- The actual key known by user, however, is kept in the index. When ckydb is initialized, the index is loaded into
  memory from the index file (a ".idx" file). The index is basically a map of `key: TIMESTAMPED-key`
- The TIMESTAMPED-key and its value are stored first in a log file (a ".log" file). This current log file has an
  in-memory copy we call `memtable`
- When the current log file exceeds a predefined size (4MBs by default), it is converted to a sorted data file (a ".cky"
  file) and `memtable` refreshed and a new log file created.
- The names of each ".cky" or ".log" file are the timestamps when they were created. Do note that conversion of ".log"
  to "cky" just changes the file extension.
- There is always one ".log" file in the database folder. If on initialization, there is no ".log" file, a new one is
  created.
- There is an in-memory sorted list of ".cky" files called `data_files` that is kept updated everytime a ".log" file is
  converted into ".cky".
- The name of the current log (`current_log_file`) file is also kept in memory, and updated when a new log file is
  created.
- There is also a ".del" file that holds all the `key: TIMESTAMPED-key` pairs that have been marked for deletion.
- At a predefined interval (5 minutes by default), a background task deletes the values from ".cky" and ".log" files
  corresponding to the `key: TIMESTAMPED-key` pairs found in the ".del" file. Each deleted pair is then removed from
  the ".del" file.
- On initial load, any keys in .del should have their values deleted in the corresponding ".log" or ".cky" files

### Operations

- On `ckydb.set(key, value)`:
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
        - if it is later or equal, `memtable` and the current log file are updated
        - else the timestamp is compared to cache's "start" and "stop" to see if it lies within the cache
        - if it exists in the cache, then the cache data and its corresponding data file are updated
        - else, the data file in which the timestamp exists is located within the data_files. This is done by finding
          the two data files between which the timestamp exists when the list is sorted in ascending order. The file to
          the left is the one containing the timestamp.
            - the key-values from the data file are then extracted and they new key-value inserted
            - the new data is then loaded into the cache
            - the new data is also loaded into the data file
    - If any error occurs on any of these steps, the preceding steps are reversed and the error returned/raised/thrown
      in the call

- On `ckydb.delete(key)`:
    - Its `key: TIMESTAMPED-key` pair is removed from the in-memory index.
    - Its `key: TIMESTAMPED-key` pair is removed from the ".idx" file
    - Its `key: TIMESTAMPED-key` is added to the ".del" file
    - If any error occurs on any of these steps, the preceding steps are reversed and the error returned/raised/thrown
      in the call

- On `ckydb.get(key)`:
    - the corresponding TIMESTAMPED key is searched for in the index
    - if the key does not exist, a NotFoundError is thrown/raised/returned.
    - if the key exists, its TIMESTAMP is extracted and checked if it is greater (later) than the name of the current
      log file.
    - if this TIMESTAMP is later, its value is quickly got from `memtable` in memory. If for some crazy reason, it does
      not exist there, a CorruptedDataError is thrown/raised/returned.
    - If this TIMESTAMP is earlier than the name of the current log file, the TIMESTAMP is compared to the range in the
      memory `cache`, if it falls there in, its value is got from `cache`. If the value is not found for some reason, a
      CorruptedDataError is thrown/raise/returned
    - Otherwise the ".cky" file whose name is earlier than the TIMESTAMP but whose neighbour to the right, in the
      in-memory sorted `data_files` list, is later than TIMESTAMP is loaded into an in-memory `cache` whose range is set
      to two ".cky" filenames between which it falls.
    - the value is then got from `cache`'s data. If it is not found for some reason, a CorruptedDataError is
      thrown/raise/returned

- On `ckydb.clear()`:
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

**Note: There is configuration that one can enable to escape the "token" in any user-defined key or value just to avoid
weird errors. However, the escaping is expensive and it is thus turned off by default.**

## Ideas For Improvement

- [ ] Explicitly allow for multiple concurrent reads (e.g. don't lock at all on read)
- [ ] Explicitly allow for conditional multiple concurrent writes (e.g. lock on key, not on store)
- [ ] Distribute the database across different machines or nodes (
  e.g. have multiple backend nodes, and let each node's timestamped key range be recorded on the master/main/gateway
  node(s). The gateway nodes themselves could be replicated. Clients read/update data through the gateway node)

### Multiple Concurrent Reads, Single Writes at a time

- [x] Have no lock on the main routine of `ckydb.get`.
  `ckydb.get` has props `index`, `memtable` and `cache` as its source of truth.
- [x] To avoid using a stale `cache` and yet also avoid data races between `store.set` and `store.get`, both, of old keys,
  we have a `cache_lock` lock. This lock is to be obtained by either `store.get` or `store.set` both for old keys
- [x] Have the same `mut_lock` lock on the `ckydb.delete` and `ckydb.set`. If you had separate locks, there would be chance
  for a data race.
- [x] For `ckydb.clear`, update `index` **first**.
- [x] For `ckydb.delete`, update `index` **last**.
- [x] For `ckydb.set` of a new key (i.e. not an update), update `index` **last**.
- [x] For `ckydb.set` of pre-existing key, update `memtable` or `cache` **last** as index would already be up-to-date.
- [x] For `store.vacuum` task and `store.delete`, there will be a `del_file_lock` within store to avoid conflicts.

## Acknowledgments

- We can do nothing without God (John 15: 5). Glory be to Him.
- Some of these ideas were adapted from [leveldb](https://github.com/google/leveldb). Thanks.

## License

Copyright (c) 2022 [Martin Ahindura](https://github.com/tinitto). All implementations are licensed under
the [MIT License](./LICENSE)