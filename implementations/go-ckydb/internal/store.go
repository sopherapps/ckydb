package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	LogFileExt   = "log"
	DelFileExt   = "del"
	DataFileExt  = "cky"
	IndexFileExt = "idx"

	IndexFilename = "index.idx"
	DelFilename   = "delete.del"

	TokenSeparator    = "$%#@*&^&"
	KeyValueSeparator = "><?&(^#"
)

type Storage interface {
	Load() error
	Set(key string, value string) error
	Get(key string) (string, error)
	Delete(key string) error
	Clear() error
	Vacuum() error
}

type Store struct {
	dbPath             string
	maxFileSizeKB      float64
	cache              *Cache
	memtable           map[string]string
	index              map[string]string
	dataFiles          []string
	currentLogFile     string
	currentLogFilePath string
	delFilePath        string
	indexFilePath      string
}

// NewStore initializes a new Store instance for the given dbPath
func NewStore(dbPath string, maxFileSizeKB float64) *Store {
	return &Store{
		dbPath:        dbPath,
		maxFileSizeKB: maxFileSizeKB,
		cache:         NewCache(nil, "0", "0"),
		delFilePath:   filepath.Join(dbPath, DelFilename),
		indexFilePath: filepath.Join(dbPath, IndexFilename),
	}
}

// Load loads the storage from disk
func (s *Store) Load() error {
	err := os.MkdirAll(s.dbPath, 0777)
	if err != nil {
		return err
	}

	err = s.createIndexFileIfNotExists()
	if err != nil {
		return err
	}

	err = s.createDelFileIfNotExists()
	if err != nil {
		return err
	}

	err = s.createLogFileIfNotExists()
	if err != nil {
		return err
	}

	// vacuum to remove keys already marked for deletion
	err = s.Vacuum()
	if err != nil {
		return err
	}

	// load the files
	err = s.loadFilePropsFromDisk()
	if err != nil {
		return err
	}

	err = s.loadIndexFromDisk()
	if err != nil {
		return err
	}

	err = s.loadMemtableFromDisk()
	if err != nil {
		return err
	}

	return nil
}

// loadFilePropsFromDisk loads the attributes that depend on the things in the folder
func (s *Store) loadFilePropsFromDisk() error {
	filesInFolder, err := GetFileOrFolderNamesInFolder(s.dbPath)
	if err != nil {
		return err
	}

	for _, filename := range filesInFolder {
		filenameLength := len(filename)
		switch filename[filenameLength-3:] {
		case LogFileExt:
			s.currentLogFile = filename[:filenameLength-4]
		case DataFileExt:
			s.dataFiles = append(s.dataFiles, filename[:filenameLength-4])
		}
	}

	// sort these data files
	sort.Strings(s.dataFiles)

	return nil
}

// Set adds or updates the value corresponding to the given key in store
// It might return an ErrCorruptedData error but if it succeeds, no error is returned
func (s *Store) Set(key string, value string) error {
	timestampedKey, err := s.getTimestampedKey(key)
	if err != nil {
		_ = s.removeTimestampedKeyForKeyIfExists(key)
		return err
	}

	err = s.saveKeyValuePair(timestampedKey, value)
	if err != nil {
		_ = s.deleteKeyValuePairIfExists(timestampedKey)
		_ = s.removeTimestampedKeyForKeyIfExists(key)
		return err
	}

	return nil
}

func (s *Store) Get(key string) (string, error) {
	panic("implement me")
}

func (s *Store) Delete(key string) error {
	panic("implement me")
}

func (s *Store) Clear() error {
	panic("implement me")
}

func (s *Store) Vacuum() error {
	keysToDelete, err := s.getKeysToDelete()
	if err != nil {
		return err
	}

	if len(keysToDelete) == 0 {
		return nil
	}

	filesInFolder, err := GetFileOrFolderNamesInFolder(s.dbPath)
	if err != nil {
		return err
	}

	for _, file := range filesInFolder {
		if file == DelFilename || file == IndexFilename {
			continue
		}

		filePath := filepath.Join(s.dbPath, file)
		err := DeleteKeyValuesFromFile(filePath, keysToDelete)
		if err != nil {
			return err
		}
	}

	delFilePath := filepath.Join(s.dbPath, DelFilename)
	_, err = os.Create(delFilePath)
	if err != nil {
		return err
	}

	return nil
}

// createIndexFileIfNotExists creates the index file if it does not exist
func (s *Store) createIndexFileIfNotExists() error {
	indexFilePath := filepath.Join(s.dbPath, IndexFilename)
	return CreateFileIfNotExist(indexFilePath)
}

// createDelFileIfNotExists creates the index file if it does not exist
func (s *Store) createDelFileIfNotExists() error {
	delFilePath := filepath.Join(s.dbPath, DelFilename)
	return CreateFileIfNotExist(delFilePath)
}

// createLogFileIfNotExists creates a new log file if it does not exist
func (s *Store) createLogFileIfNotExists() error {
	filesInFolder, err := GetFileOrFolderNamesInFolder(s.dbPath)
	if err != nil {
		return err
	}

	for _, filename := range filesInFolder {
		if strings.HasSuffix(filename, LogFileExt) {
			s.currentLogFilePath = filepath.Join(s.dbPath, filename)
			return nil
		}
	}

	return s.createNewLogFile()
}

// createNewLogFile creates a new log file basing on the current timestamp
func (s *Store) createNewLogFile() error {
	logFilename := fmt.Sprintf("%d", time.Now().UnixNano())
	logFilePath := filepath.Join(s.dbPath, fmt.Sprintf("%s.%s", logFilename, LogFileExt))

	err := CreateFileIfNotExist(logFilePath)
	if err != nil {
		return err
	}

	s.currentLogFile = logFilename
	s.currentLogFilePath = logFilePath
	return nil
}

// loadIndexFromDisk loads the index from the index file
func (s *Store) loadIndexFromDisk() error {
	idxFilePath := filepath.Join(s.dbPath, IndexFilename)
	data, err := os.ReadFile(idxFilePath)
	if err != nil {
		return err
	}

	dataAsMap, err := ExtractKeyValuesFromByteArray(data)
	if err != nil {
		return err
	}

	s.index = dataAsMap
	return nil
}

// loadMemtableFromDisk loads the memtable from the current log file
func (s *Store) loadMemtableFromDisk() error {
	logFilePath := filepath.Join(s.dbPath, fmt.Sprintf("%s.%s", s.currentLogFile, LogFileExt))
	data, err := os.ReadFile(logFilePath)
	if err != nil {
		return err
	}

	dataAsMap, err := ExtractKeyValuesFromByteArray(data)
	if err != nil {
		return err
	}

	s.memtable = dataAsMap
	return nil
}

// getKeysToDelete reads the del file and gets the keys to be deleted
func (s *Store) getKeysToDelete() ([]string, error) {
	delFilePath := filepath.Join(s.dbPath, DelFilename)
	data, err := os.ReadFile(delFilePath)
	if err != nil {
		return nil, err
	}

	return ExtractTokensFromByteArray(data)
}

// getTimestampedKey gets the timestamped key corresponding to the given key in the index
// If there is none, it creates a new timestamped key and adds it to the index and the index file
func (s *Store) getTimestampedKey(key string) (string, error) {
	timestampedKey, ok := s.index[key]

	if !ok {
		timestampedKey = fmt.Sprintf("%d-%s", time.Now().UnixNano(), key)
		s.index[key] = timestampedKey

		f, err := os.OpenFile(s.indexFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			return "", err
		}
		defer func() { _ = f.Close() }()

		data := fmt.Sprintf("%s%s%s%s", key, KeyValueSeparator, timestampedKey, TokenSeparator)
		_, err = f.WriteString(data)
		if err != nil {
			return "", err
		}
	}

	return timestampedKey, nil
}

// removeTimestampedKeyForKeyIfExists removes the key and timestamped key from the index
// and the index file if it exists
func (s *Store) removeTimestampedKeyForKeyIfExists(key string) error {
	_, ok := s.index[key]
	if !ok {
		return nil
	}

	err := DeleteKeyValuesFromFile(s.indexFilePath, []string{key})
	if err != nil {
		return err
	}

	return nil
}

// saveKeyValuePair saves the key value pair in memtable and log file if it is newer than log file
// or in cache and in the corresponding dataFile if the key is old
func (s *Store) saveKeyValuePair(timestampedKey string, value string) error {
	if timestampedKey >= s.currentLogFile {
		return s.saveKeyValueToMemtable(timestampedKey, value)
	}

	if !s.cache.IsInRange(timestampedKey) {
		err := s.loadCacheContainingKey(timestampedKey)
		if err != nil {
			return err
		}
	}

	return s.saveKeyValueToCache(timestampedKey, value)
}

// saveKeyValueToMemtable saves the key value pair to memtable and persists memtable
// to current log file
func (s *Store) saveKeyValueToMemtable(timestampedKey string, value string) error {
	s.memtable[timestampedKey] = value
	err := PersistMapDataToFile(s.memtable, s.currentLogFilePath)
	if err != nil {
		return err
	}

	err = s.rollLogFileIfTooBig()
	if err != nil {
		return err
	}

	return nil
}

// saveKeyValueToCache saves the key value pair to cache and persists cache
// to corresponding data file
func (s *Store) saveKeyValueToCache(timestampedKey string, value string) error {
	s.cache.Update(timestampedKey, value)
	return s.persistCacheToDisk()
}

// persistCacheToDisk persists the current cache to its corresponding data file
func (s *Store) persistCacheToDisk() error {
	dataFilePath := filepath.Join(s.dbPath, fmt.Sprintf("%s.%s", s.cache.start, DataFileExt))
	return PersistMapDataToFile(s.cache.data, dataFilePath)
}

// rollLogFileIfTooBig rolls the log file if it has exceeded the maximum size it should have
func (s *Store) rollLogFileIfTooBig() error {
	logFileSize, err := GetFileSize(s.currentLogFilePath)
	if err != nil {
		return err
	}

	if logFileSize >= s.maxFileSizeKB {
		newDataFilename := fmt.Sprintf("%s.%s", s.currentLogFile, DataFileExt)
		err = os.Rename(s.currentLogFilePath, filepath.Join(s.dbPath, newDataFilename))
		if err != nil {
			return err
		}

		s.memtable = map[string]string{}
		s.dataFiles = append(s.dataFiles, s.currentLogFile)
		// ensure these data files are sorted
		sort.Strings(s.dataFiles)

		err = s.createNewLogFile()
		if err != nil {
			return err
		}
	}

	return nil
}

// getTimestampRangeForKey returns the range of timestamps between which
// the key lies. The timestamps are got from the names of the data files and the current log file
func (s *Store) getTimestampRangeForKey(key string) *Range {
	numberOfTimestamps := len(s.dataFiles) + 1
	timestamps := make([]string, numberOfTimestamps)
	copy(timestamps, s.dataFiles)
	timestamps[numberOfTimestamps-1] = s.currentLogFile

	for i := 1; i < numberOfTimestamps; i++ {
		current := timestamps[i]
		if current > key {
			return &Range{Start: timestamps[i-1], End: current}
		}

	}

	return nil
}

// loadCacheContainingKey loads the cache with data containing the timestampedKey
func (s *Store) loadCacheContainingKey(timestampedKey string) error {
	timestampRange := s.getTimestampRangeForKey(timestampedKey)
	if timestampRange == nil {
		return ErrCorruptedData
	}

	filePath := filepath.Join(s.dbPath, fmt.Sprintf("%s.%s", timestampRange.Start, DataFileExt))
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	mapData, err := ExtractKeyValuesFromByteArray(data)
	if err != nil {
		return err
	}

	s.cache = NewCache(mapData, timestampRange.Start, timestampRange.End)
	return nil
}

// deleteKeyValuePairIfExists deletes the given key value pair from
// the index, the cache or the memtable, the log file or any data file
func (s *Store) deleteKeyValuePairIfExists(timestampedKey string) error {
	if s.cache.IsInRange(timestampedKey) {
		s.cache.Remove(timestampedKey)
		return s.persistCacheToDisk()
	} else if timestampedKey >= s.currentLogFile {
		delete(s.memtable, timestampedKey)
		return PersistMapDataToFile(s.memtable, s.currentLogFilePath)
	}

	return nil
}
