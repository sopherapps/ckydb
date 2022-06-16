# ckydb

A simple fast memory-first thread-safe (or goroutine-safe for Go) key-value embedded database that persist data on disk.

It is read as 'skydb' This the python implementation of ckydb

## Quick Start

- Create a new project and activate your virtual environment

```shell
mkdir ckydb_example
cd ckydb_example
python3 -m venv env
source env/bin/activae
```

- Install ckydb

```shell
pip install ckydb
```

- Create a main.py file in the project folder and add the following code

```python
if __name__ == '__main__':
    import ckydb

    keys = ["hey", "hi", "salut", "bonjour", "hola", "oi", "mulimuta"]
    values = ["English", "English", "French", "French", "Spanish", "Portuguese", "Runyoro"]
    db = ckydb.connect("db",
                       max_file_size_kb=(4 * 1024),
                       vacuum_interval_sec=(5 * 60),
                       should_sanitize=False)

    # setting the keys
    for k, v in zip(keys, values):
        db.set(k, v)

    for i, k in enumerate(keys):
        assert values[i] == db.get(k)

    # updating keys
    new_values = ["Jane", "John", "Jean", "Marie", "Santos", "Ronaldo", "Aliguma"]
    for k, v in zip(keys, new_values):
        db.set(k, v)

    for i, k in enumerate(keys):
        assert new_values[i] == db.get(k)

    # deleting the keys
    for k in keys[:2]:
        db.delete(k)

    for k, v in zip(keys[2:], new_values[2:]):
        assert v == db.get(k)

    errors = []

    for k in keys[:2]:
        try:
            v = db.get(k)
        except ckydb.exc.NotFoundError as exc:
            errors.append(exc)

    assert len(errors) == len(keys[:2])

    # clear the database
    errors.clear()
    db.clear()

    for k in keys:
        try:
            v = db.get(k)
        except ckydb.exc.NotFoundError as exc:
            errors.append(exc)

    assert len(errors) == len(keys)

```

- Run the `main.py` module and observe the terminal

```shell
python main.py
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
- It is possible for ckydb to be configured to sanitize all keys and values to replace any potential occurrences of the
  token used to separate the key-value pairs on file. This avoids weird DataCorruptionError's. By default, this is
  turned off as it impacts performance heavily.

### Operations

- On `ckydb.set(key, value)`:
    - the corresponding TIMESTAMPED key is searched for in the index
    - if the key does not exist, a new TIMESTAMPED key is created and added to the index with its user-defined key
    - this TIMESTAMPED key and its value are then added to `memtable`.
    - the user-defined key and its TIMESTAMPED key are then added to the index file (".idx")
    - this TIMESTAMPED key and its value are then added to the current log file (".log")
    - If any error occurs on any of these steps, the preceding steps are reversed and the error returned/raised/thrown
      in the call

- On `ckydb.delete(key)`:
    - Its `key: TIMESTAMPED-key` pair is removed from the in-memory index.
    - Its `key: TIMESTAMPED-key` pair is removed from the ".idx" file
    - Its `key: TIMESTAMPED-key` is add to the ".del" file
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
goat[><?&(^#]1655304770518678-goat{&*/%}hen[><?&(^#]1655304670510698-hen{&*/%}pig[><?&(^#]1655304770534578-pig{&*/%}fish[><?&(^#]1655303775538278-fish
```

- The file format of the ".del" files is just "TIMESTAMPED-key<token>" separated by a
  unique token e.g. "{&*/%}"

```
1655304770518678-goat{&*/%}1655304670510698-hen{&*/%}1655304770534578-pig{&*/%}1655303775538278-fish
```

- The file format of the ".log" and ".cky" files is just  "TIMESTAMPED-key<key_value_separator>value<token>" separated by a unique token
  e.g. "{&*/%}" and a key_value_separator like "[><?&(^#]"

```
1655304770518678-goat[><?&(^#]678 months{&*/%}1655304670510698-hen[><?&(^#]567 months{&*/%}1655304770534578-pig[><?&(^#]70 months{&*/%}1655303775538278-fish[><?&(^#]8990 months
```

**Note: There is configuration that one can enable to escape the "token" in any user-defined key or value just to avoid
weird errors. However, the escaping is expensive and it is thus turned off by default.**

## Acknowledgments

- We can do nothing without God (John 15: 5). Glory be to Him.
- Some of these ideas were adapted from [leveldb](https://github.com/google/leveldb). Thanks.

## License

Copyright (c) 2022 [Martin Ahindura](https://github.com/tinitto). All implementations are licensed to Licensed under
the [MIT License](./LICENSE)