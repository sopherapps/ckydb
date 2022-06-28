package internal

import (
	"fmt"
	"os"
	"path/filepath"
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
	dbPath         string
	maxFileSizeKB  float64
	cache          *Cache
	memtable       map[string]string
	index          map[string]string
	dataFiles      []string
	currentLogFile string
}

// NewStore initializes a new Store instance for the given dbPath
func NewStore(dbPath string, maxFileSizeKB float64) *Store {
	return &Store{
		dbPath:        dbPath,
		maxFileSizeKB: maxFileSizeKB,
		cache:         NewCache(nil, "0", "0"),
	}
}

// Load loads the storage from disk
func (s *Store) Load() error {
	// create files if they don't exist
	err := s.createIndexFileIfNotExists()
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

	return nil
}

// Set adds or updates the value corresponding to the given key in store
// It might return an ErrCorruptedData error but if it succeeds, no error is returned
func (s *Store) Set(key string, value string) error {
	panic("implement me")
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

	s.index = dataAsMap
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
