# ckydb

A simple fast memory-first thread-safe (or goroutine-safe for Go) key-value embedded database that persists data on disk.

It is read as 'skydb' This the python implementation of ckydb

## Quick Start

- Create a new project and activate your virtual environment

```shell
mkdir ckydb_example
cd ckydb_example
python3 -m venv env
source env/bin/activate
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
    with ckydb.connect("db", max_file_size_kb=(4 * 1024), vacuum_interval_sec=(5 * 60)) as db:
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

## How to Run Tests

- Clone the repo

```shell
git clone git@github.com:sopherapps/ckydb.git
```

- Enter the python implementation folder

```shell
cd ckydb/implementations/py_ckydb
```

- Create and activate the python 3.7+ virtual environment

```shell
python3 -m venv env
source env/bin/activate
```

- Run the tests and benchmarks command

```shell
pip install -r requirements.tx
pytest
```

- Or run the benchmarks tests alone

```shell
pytest tests/test_benchmarks.py
```

- The latest benchmarks are as shown below:

```shell

----------------------------------------------------------------------------------------------------------------------- benchmark: 27 tests -----------------------------------------------------------------------------------------------------------------------
Name (time in ns)                                            Min                        Max                      Mean                    StdDev                    Median                     IQR            Outliers             OPS            Rounds  Iterations
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_get[oi-Portuguese]                       855.0000 (1.0)          58,647.0000 (2.08)           995.9147 (1.07)           929.1278 (4.88)           905.0000 (1.0)           21.0000 (1.62)     524;4221  1,004,102.0465 (0.94)      49315           1
test_benchmark_get[hi-English]                          869.0000 (1.02)         40,734.0000 (1.45)           992.1055 (1.07)           732.1780 (3.84)           931.0000 (1.03)          23.0000 (1.77)    1201;3948  1,007,957.2857 (0.94)      72802           1
test_benchmark_get[hey-English]                         872.0000 (1.02)         30,506.0000 (1.08)           931.2074 (1.0)            301.4543 (1.58)           919.0000 (1.02)          13.0000 (1.0)      306;3232  1,073,874.5983 (1.0)       47916           1
test_benchmark_get[salut-French]                        878.0000 (1.03)        612,930.0000 (21.76)        1,039.3895 (1.12)         2,861.8812 (15.02)          921.0000 (1.02)          18.0000 (1.38)     281;6777    962,103.2398 (0.90)      73218           1
test_benchmark_get[hola-Spanish]                        881.0000 (1.03)         69,487.0000 (2.47)           981.5411 (1.05)           699.9669 (3.67)           921.0000 (1.02)          24.0000 (1.85)    1697;4720  1,018,806.0712 (0.95)      75678           1
test_benchmark_get[mulimuta-Runyoro]                    891.0000 (1.04)         28,165.0000 (1.0)            948.8532 (1.02)           190.5828 (1.0)            940.0000 (1.04)          20.0000 (1.54)     281;1080  1,053,903.7993 (0.98)      40036           1
test_benchmark_get[bonjour-French]                      898.0000 (1.05)         46,728.0000 (1.66)           955.7240 (1.03)           260.8250 (1.37)           941.0000 (1.04)          16.0000 (1.23)     987;3054  1,046,327.1727 (0.97)      74756           1
test_benchmark_set[mulimuta-Runyoro]                175,386.0000 (205.13)    1,613,037.0000 (57.27)      249,949.7968 (268.41)      84,215.5473 (441.88)     234,083.0000 (258.66)    55,684.0000 (>1000.0)   119;111      4,000.8034 (0.00)       1324           1
test_benchmark_set[hola-Spanish]                    179,401.0000 (209.83)      927,031.0000 (32.91)      235,646.8298 (253.06)      42,001.4767 (220.38)     233,877.0000 (258.43)    48,001.2500 (>1000.0)    218;38      4,243.6387 (0.00)       1187           1
test_benchmark_set[hi-English]                      179,746.0000 (210.23)    2,421,578.0000 (85.98)      238,329.7016 (255.94)     105,376.9807 (552.92)     228,807.0000 (252.83)    49,351.5000 (>1000.0)     31;53      4,195.8681 (0.00)       1461           1
test_benchmark_update[bonjour-French-Ronaldo]       180,299.0000 (210.88)    2,359,103.0000 (83.76)      243,184.0168 (261.15)      86,186.1452 (452.22)     235,706.0000 (260.45)    46,876.2500 (>1000.0)     65;83      4,112.1124 (0.00)       1431           1
test_benchmark_set[bonjour-French]                  180,911.0000 (211.59)    2,469,679.0000 (87.69)      232,491.9718 (249.67)      99,442.1938 (521.78)     219,994.0000 (243.09)    51,377.5000 (>1000.0)     29;37      4,301.2238 (0.00)       1381           1
test_benchmark_set[hey-English]                     181,285.0000 (212.03)    1,080,837.0000 (38.38)      236,476.3138 (253.95)      45,324.6426 (237.82)     230,549.0000 (254.75)    48,328.5000 (>1000.0)    196;48      4,228.7533 (0.00)       1300           1
test_benchmark_set[salut-French]                    181,297.0000 (212.04)    3,221,152.0000 (114.37)     246,632.1208 (264.85)     111,666.8726 (585.92)     234,435.0000 (259.04)    51,332.0000 (>1000.0)     42;67      4,054.6219 (0.00)       1382           1
test_benchmark_update[hey-English-Jane]             181,637.0000 (212.44)    3,033,289.0000 (107.70)     258,670.7316 (277.78)     139,835.8795 (733.73)     237,851.0000 (262.82)    56,673.0000 (>1000.0)     54;98      3,865.9186 (0.00)       1386           1
test_benchmark_update[salut-French-Juan]            183,118.0000 (214.17)    8,249,639.0000 (292.90)     258,608.0518 (277.71)     277,312.5313 (>1000.0)    235,826.5000 (260.58)    50,069.0000 (>1000.0)     12;58      3,866.8556 (0.00)       1042           1
test_benchmark_set[oi-Portuguese]                   183,682.0000 (214.83)    1,660,508.0000 (58.96)      258,216.0094 (277.29)     108,366.4193 (568.61)     239,684.0000 (264.84)    60,371.7500 (>1000.0)     78;85      3,872.7266 (0.00)       1377           1
test_benchmark_update[hola-Spanish-Aliguma]         183,997.0000 (215.20)   19,994,117.0000 (709.89)     318,343.6547 (341.86)   1,000,775.7719 (>1000.0)    234,927.0000 (259.59)    49,706.0000 (>1000.0)     14;98      3,141.2594 (0.00)       1225           1
test_benchmark_update[hi-English-John]              187,160.0000 (218.90)    1,384,953.0000 (49.17)      245,119.3383 (263.23)      63,823.1939 (334.88)     239,437.0000 (264.57)    49,916.2500 (>1000.0)    112;69      4,079.6455 (0.00)       1271           1
test_benchmark_delete[salut-French]                 268,409.0000 (313.93)    1,808,337.0000 (64.21)      380,571.3900 (408.69)     229,426.7356 (>1000.0)    330,050.0000 (364.70)    76,610.0000 (>1000.0)       3;5      2,627.6279 (0.00)        100           1
test_benchmark_delete[hola-Spanish]                 271,768.0000 (317.86)   10,747,689.0000 (381.60)     604,214.9000 (648.85)   1,270,686.7786 (>1000.0)    352,695.0000 (389.72)    96,248.0000 (>1000.0)      4;13      1,655.0403 (0.00)        100           1
test_benchmark_delete[oi-Portuguese]                274,435.0000 (320.98)    2,303,923.0000 (81.80)      552,187.3900 (592.98)     323,658.2281 (>1000.0)    461,320.5000 (509.75)   226,026.5000 (>1000.0)       8;7      1,810.9794 (0.00)        100           1
test_benchmark_delete[hey-English]                  274,868.0000 (321.48)      463,587.0000 (16.46)      338,799.0800 (363.83)      44,700.2670 (234.55)     332,542.5000 (367.45)    73,487.0000 (>1000.0)      40;0      2,951.6019 (0.00)        100           1
test_benchmark_delete[mulimuta-Runyoro]             276,279.0000 (323.13)    1,447,257.0000 (51.38)      427,976.1000 (459.59)     170,256.8707 (893.35)     391,328.5000 (432.41)   179,640.5000 (>1000.0)      13;3      2,336.5791 (0.00)        100           1
test_benchmark_delete[bonjour-French]               277,114.0000 (324.11)    2,401,331.0000 (85.26)      386,813.1500 (415.39)     246,937.6971 (>1000.0)    344,319.5000 (380.46)    72,233.5000 (>1000.0)       3;4      2,585.2275 (0.00)        100           1
test_benchmark_delete[hi-English]                   282,098.0000 (329.94)      702,759.0000 (24.95)      357,658.7100 (384.08)      58,772.9159 (308.39)     347,696.0000 (384.19)    65,359.0000 (>1000.0)      19;4      2,795.9615 (0.00)        100           1
test_benchmark_clear                              1,066,242.0000 (>1000.0)   2,522,719.0000 (89.57)    1,440,370.8200 (>1000.0)    327,961.1290 (>1000.0)  1,309,266.5000 (>1000.0)  363,294.5000 (>1000.0)      24;5        694.2657 (0.00)        100           1
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Legend:
  Outliers: 1 Standard Deviation from Mean; 1.5 IQR (InterQuartile Range) from 1st Quartile and 3rd Quartile.
  OPS: Operations Per Second, computed as 1 / Mean
============================= 27 passed in 12.70s ==============================
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

- [x] Explicitly allow for multiple concurrent reads (e.g. don't lock at all on read)
- [ ] Explicitly allow for conditional multiple concurrent writes (e.g. lock on key, not on store)
- [ ] Distribute the database across different machines or nodes (
  e.g. have multiple backend nodes, and let each node's timestamped key range be recorded on the master/main/gateway
  node(s). The gateway nodes themselves could be replicated. Clients read/update data through the gateway node)

### Multiple Concurrent Reads, Single Writes at a time

- Have no lock on the main routine of `ckydb.get`.
  `ckydb.get` has props `index`, `memtable` and `cache` as its source of truth.
- To avoid using a stale `cache` and yet also avoid data races between `store.set` and `store.get`, both, of old keys,
  we have a `cache_lock` lock. This lock is to be obtained by either `store.get` or `store.set` both for old keys
- Have the same `mut_lock` lock on the `ckydb.delete` and `ckydb.set`. If you had separate locks, there would be chance
  for a data race.
- For `ckydb.clear`, update `index` **first**.
- For `ckydb.delete`, update `index` **last**.
- For `ckydb.set` of a new key (i.e. not an update), update `index` **last**.
- For `ckydb.set` of pre-existing key, update `memtable` or `cache` **last** as index would already be up-to-date.
- For `store.vacuum` task and `store.delete`, there will be a `del_file_lock` within store to avoid conflicts.

## Acknowledgments

- We can do nothing without God (John 15: 5). Glory be to Him.
- Some of these ideas were adapted from [leveldb](https://github.com/google/leveldb). Thanks.

## License

Copyright (c) 2022 [Martin Ahindura](https://github.com/tinitto). All implementations are licensed under
the [MIT License](./LICENSE)