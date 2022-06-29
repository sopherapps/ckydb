package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStore(t *testing.T) {
	dbPath, err := filepath.Abs("testStoreDb")
	if err != nil {
		t.Fatal(err)
	}

	maxFileSizeKB := 320.0 / 1024
	logFilename := "1655375171402014000.log"
	indexFilename := "index.idx"
	delFilename := "delete.del"
	indexFilePath := filepath.Join(dbPath, indexFilename)
	delFilePath := filepath.Join(dbPath, delFilename)
	logFilePath := filepath.Join(dbPath, logFilename)
	dataFiles := []string{
		"1655375120328185000.cky",
		"1655375120328186000.cky",
	}
	sort.Strings(dataFiles)

	t.Run("LoadShouldUpdateMemoryPropsFromDataOnDisk", func(t *testing.T) {
		expectedCache := NewCache(nil, "0", "0")
		expectedIndex := map[string]string{
			"cow":  "1655375120328185000-cow",
			"dog":  "1655375120328185100-dog",
			"goat": "1655404770518678-goat",
			"hen":  "1655404670510698-hen",
			"pig":  "1655404770534578-pig",
			"fish": "1655403775538278-fish",
		}
		expectedMemtable := map[string]string{
			"1655404770518678-goat": "678 months",
			"1655404670510698-hen":  "567 months",
			"1655404770534578-pig":  "70 months",
			"1655403775538278-fish": "8990 months",
		}
		expectedDataFiles := make([]string, len(dataFiles))
		expectedCurrentLogFile := strings.TrimRight(logFilename, ".log")
		for i, file := range dataFiles {
			expectedDataFiles[i] = strings.TrimRight(file, ".cky")
		}

		err := AddDummyFileDataInDb(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ClearDummyFileDataInDb(dbPath) }()

		store := NewStore(dbPath, maxFileSizeKB)
		err = store.Load()
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, expectedCache, store.cache)
		assert.Equal(t, expectedMemtable, store.memtable)
		assert.Equal(t, expectedIndex, store.index)
		assert.Equal(t, expectedDataFiles, store.dataFiles)
		assert.Equal(t, expectedCurrentLogFile, store.currentLogFile)
		assert.Equal(t, indexFilePath, store.indexFilePath)
		assert.Equal(t, logFilePath, store.currentLogFilePath)
		assert.Equal(t, delFilePath, store.delFilePath)
	})

	t.Run("LoadShouldCreateDatabaseFolderWithIndexAndDelFilesIfNotExist", func(t *testing.T) {
		expectedCache := NewCache(nil, "0", "0")
		expectedFiles := []string{DelFilename, IndexFilename}
		emptyMap := map[string]string{}
		var emptyList []string

		err := ClearDummyFileDataInDb(dbPath)
		if err != nil {
			t.Fatal(err)
		}

		store := NewStore(dbPath, maxFileSizeKB)
		err = store.Load()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ClearDummyFileDataInDb(dbPath) }()

		currentLogFilename := fmt.Sprintf("%s.log", store.currentLogFile)
		expectedFiles = append(expectedFiles, currentLogFilename)
		expectedCurrentLogFilePath := filepath.Join(dbPath, currentLogFilename)
		actualFiles, err := GetFileOrFolderNamesInFolder(dbPath)
		if err != nil {
			t.Fatal(err)
		}

		sort.Strings(expectedFiles)
		sort.Strings(actualFiles)

		assert.Equal(t, expectedCache, store.cache)
		assert.NotEqual(t, "", store.currentLogFile)
		assert.Equal(t, emptyMap, store.index)
		assert.Equal(t, emptyMap, store.memtable)
		assert.Equal(t, emptyList, store.dataFiles)
		assert.Equal(t, expectedFiles, actualFiles)
		assert.Equal(t, indexFilePath, store.indexFilePath)
		assert.Equal(t, expectedCurrentLogFilePath, store.currentLogFilePath)
		assert.Equal(t, delFilePath, store.delFilePath)
	})

	t.Run("SetNewKeyShouldAddKeyValueToMemtableAndIndexAndLogFile", func(t *testing.T) {
		key, value := time.Now().Format("2006-01-02 15:04:05"), "foo"

		err := AddDummyFileDataInDb(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ClearDummyFileDataInDb(dbPath) }()

		store := NewStore(dbPath, maxFileSizeKB)
		err = store.Load()
		if err != nil {
			t.Fatal(err)
		}

		err = store.Set(key, value)
		if err != nil {
			t.Fatal(err)
		}

		timestampedKey := store.index[key]
		expectedIndexFileEntry := fmt.Sprintf("%s%s%s%s", key, KeyValueSeparator, timestampedKey, TokenSeparator)
		expectedLogFileEntry := fmt.Sprintf("%s%s%s%s", timestampedKey, KeyValueSeparator, value, TokenSeparator)

		valueInMemtable := store.memtable[timestampedKey]
		indexFileContent, err := ReadFileToString(indexFilePath)
		if err != nil {
			t.Fatal(err)
		}
		logFileContent, err := ReadFileToString(logFilePath)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, value, valueInMemtable)
		assert.Contains(t, indexFileContent, expectedIndexFileEntry)
		assert.Contains(t, logFileContent, expectedLogFileEntry)
	})

	t.Run("SetSameRecentKeyShouldUpdateKeyValueInMemtableAndLogFile", func(t *testing.T) {
		key, value, newValue := time.Now().Format("2006-01-02 15:04:05"), "foo", "hello-world"

		err := AddDummyFileDataInDb(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ClearDummyFileDataInDb(dbPath) }()

		store := NewStore(dbPath, maxFileSizeKB)
		err = store.Load()
		if err != nil {
			t.Fatal(err)
		}

		err = store.Set(key, value)
		if err != nil {
			t.Fatal(err)
		}
		err = store.Set(key, newValue)
		if err != nil {
			t.Fatal(err)
		}

		timestampedKey := store.index[key]
		expectedLogFileEntry := fmt.Sprintf("%s%s%s%s", timestampedKey, KeyValueSeparator, newValue, TokenSeparator)
		valueInMemtable := store.memtable[timestampedKey]
		logFileContent, err := ReadFileToString(logFilePath)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, newValue, valueInMemtable)
		assert.Contains(t, logFileContent, expectedLogFileEntry)
	})

	t.Run("SetOldKeyShouldUpdateKeyValueInCacheAndDataFile", func(t *testing.T) {
		key, value := "cow", "foo-again"
		dataFilePath := filepath.Join(dbPath, dataFiles[0])

		err := AddDummyFileDataInDb(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ClearDummyFileDataInDb(dbPath) }()

		store := NewStore(dbPath, maxFileSizeKB)
		err = store.Load()
		if err != nil {
			t.Fatal(err)
		}

		err = store.Set(key, value)
		if err != nil {
			t.Fatal(err)
		}

		timestampedKey := store.index[key]
		expectedDataFileEntry := fmt.Sprintf("%s%s%s", timestampedKey, KeyValueSeparator, value)
		valueInCache := store.cache.data[timestampedKey]
		dataFileContent, err := ReadFileToString(dataFilePath)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, value, valueInCache)
		assert.Contains(t, dataFileContent, expectedDataFileEntry)
	})

	t.Run("GetNewKeyShouldGetValueFromMemtable", func(t *testing.T) {

	})

	t.Run("GetOldKeyShouldUpdateCacheFromDiskAndGetValueFromCache", func(t *testing.T) {

	})

	t.Run("GetOldKeyAgainShouldPickKeyValueFromMemoryCache", func(t *testing.T) {

	})

	t.Run("GetNonExistentKeyThrowsNotFoundError", func(t *testing.T) {

	})

	t.Run("DeleteKeyShouldRemoveKeyFromIndexAndAddItToDelFile", func(t *testing.T) {

	})

	t.Run("DeleteNonExistentKeyThrowsNotFoundError", func(t *testing.T) {

	})

	t.Run("ClearShouldDeleteAllDataOnDisk", func(t *testing.T) {

	})

	t.Run("VacuumShouldDeleteAllKeyValuesInDataFilesAndLogFileForAllKeysInDelFile", func(t *testing.T) {
		expectedLogFileContent := "1655404770518678-goat><?&(^#678 months$%#@*&^&1655404670510698-hen><?&(^#567 months$%#@*&^&1655404770534578-pig><?&(^#70 months$%#@*&^&1655403775538278-fish><?&(^#8990 months$%#@*&^&"
		expectedDataFileContent := []string{
			"1655375120328185000-cow><?&(^#500 months$%#@*&^&1655375120328185100-dog><?&(^#23 months$%#@*&^&", ""}
		expectedDelFileContent := ""

		delFilePath := filepath.Join(dbPath, delFilename)
		logFilePath := filepath.Join(dbPath, logFilename)
		dataFilePaths := make([]string, len(dataFiles))

		for i, file := range dataFiles {
			dataFilePaths[i] = filepath.Join(dbPath, file)
		}

		err := AddDummyFileDataInDb(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ClearDummyFileDataInDb(dbPath) }()

		store := NewStore(dbPath, maxFileSizeKB)
		err = store.Vacuum()
		if err != nil {
			t.Fatal(err)
		}

		dataFileContent := make([]string, len(dataFiles))
		for i, path := range dataFilePaths {
			dataFileContent[i], err = ReadFileToString(path)
			if err != nil {
				t.Fatal(err)
			}
		}

		logFileContent, err := ReadFileToString(logFilePath)
		if err != nil {
			t.Fatal(err)
		}

		delFileContent, err := ReadFileToString(delFilePath)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, expectedLogFileContent, logFileContent)
		assert.Equal(t, expectedDelFileContent, delFileContent)
		assert.Equal(t, expectedDataFileContent, dataFileContent)

	})

	t.Run("VacuumShouldDoNothingIfDelFileIsEmpty", func(t *testing.T) {
		expectedLogFileContent := "1655404770518678-goat><?&(^#678 months$%#@*&^&1655404670510698-hen><?&(^#567 months$%#@*&^&1655404770534578-pig><?&(^#70 months$%#@*&^&1655403775538278-fish><?&(^#8990 months$%#@*&^&1655403795838278-foo><?&(^#890 months$%#@*&^&"
		expectedDataFileContent := []string{
			"1655375120328185000-cow><?&(^#500 months$%#@*&^&1655375120328185100-dog><?&(^#23 months$%#@*&^&", "1655375171402014000-bar><?&(^#foo$%#@*&^&"}
		expectedDelFileContent := ""

		delFilePath := filepath.Join(dbPath, delFilename)
		logFilePath := filepath.Join(dbPath, logFilename)
		dataFilePaths := make([]string, len(dataFiles))

		for i, file := range dataFiles {
			dataFilePaths[i] = filepath.Join(dbPath, file)
		}

		err := AddDummyFileDataInDb(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ClearDummyFileDataInDb(dbPath) }()

		// clear delete file
		_, err = os.Create(delFilePath)
		if err != nil {
			t.Fatal(err)
		}

		store := NewStore(dbPath, maxFileSizeKB)
		err = store.Vacuum()
		if err != nil {
			t.Fatal(err)
		}

		dataFileContent := make([]string, len(dataFiles))
		for i, path := range dataFilePaths {
			dataFileContent[i], err = ReadFileToString(path)
			if err != nil {
				t.Fatal(err)
			}
		}

		logFileContent, err := ReadFileToString(logFilePath)
		if err != nil {
			t.Fatal(err)
		}

		delFileContent, err := ReadFileToString(delFilePath)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, expectedLogFileContent, logFileContent)
		assert.Equal(t, expectedDelFileContent, delFileContent)
		assert.Equal(t, expectedDataFileContent, dataFileContent)
	})
}
