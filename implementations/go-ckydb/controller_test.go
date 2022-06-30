package ckydb

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/sopherapps/ckydb/implementations/go-ckydb/internal"
	"github.com/stretchr/testify/assert"
)

func TestCkydb(t *testing.T) {
	dbPath, err := filepath.Abs("testControllerDb")
	if err != nil {
		t.Fatal(err)
	}
	vacuumIntervalSec := 2.0
	maxFileSizeKB := 320.0 / 1024
	testRecords := map[string]string{
		"hey":      "English",
		"hi":       "English",
		"salut":    "French",
		"bonjour":  "French",
		"hola":     "Spanish",
		"oi":       "Portuguese",
		"mulimuta": "Runyoro",
	}

	t.Run("ConnectShouldCallOpen", func(t *testing.T) {
		db, err := connectToTestDb(dbPath, maxFileSizeKB, vacuumIntervalSec)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = db.Close()
			_ = internal.ClearDummyFileDataInDb(dbPath)
		}()

		assert.Greater(t, len(db.tasks), 0)
		for _, task := range db.tasks {
			assert.True(t, task.IsRunning())
		}
	})

	t.Run("OpenShouldStartAllBackgroundTasks", func(t *testing.T) {
		db, err := newCkydb(dbPath, maxFileSizeKB, vacuumIntervalSec)
		if err != nil {
			t.Fatal(err)
		}

		err = db.Open()
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = db.Close()
			_ = internal.ClearDummyFileDataInDb(dbPath)
		}()

		assert.Greater(t, len(db.tasks), 0)
		for _, task := range db.tasks {
			assert.True(t, task.IsRunning())
		}
	})

	t.Run("CloseShouldStopAllBackgroundTasks", func(t *testing.T) {
		db, err := connectToTestDb(dbPath, maxFileSizeKB, vacuumIntervalSec)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = internal.ClearDummyFileDataInDb(dbPath)
		}()

		err = db.Close()
		if err != nil {
			t.Fatal(err)
		}

		assert.Greater(t, len(db.tasks), 0)
		for _, task := range db.tasks {
			assert.False(t, task.IsRunning())
		}
	})

	t.Run("SetNewKeyShouldAddKeyValueToStore", func(t *testing.T) {
		db, err := connectToTestDb(dbPath, maxFileSizeKB, vacuumIntervalSec)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = db.Close()
			_ = internal.ClearDummyFileDataInDb(dbPath)
		}()

		for key, value := range testRecords {
			err = db.Set(key, value)
			if err != nil {
				t.Fatal(err)
			}
		}

		for k, v := range testRecords {
			value, err := db.Get(k)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, v, value)
		}
	})

	t.Run("SetOldKeyShouldUpdateOldKeyWithValue", func(t *testing.T) {
		oldRecords := make(map[string]string, len(testRecords))
		for k, v := range testRecords {
			oldRecords[k] = v
		}
		updates := map[string]string{
			"hey":      "Jane",
			"hi":       "John",
			"salut":    "Jean",
			"oi":       "Ronaldo",
			"mulimuta": "Aliguma",
		}

		db, err := connectToTestDb(dbPath, maxFileSizeKB, vacuumIntervalSec)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = db.Close()
			_ = internal.ClearDummyFileDataInDb(dbPath)
		}()

		for k, v := range oldRecords {
			err = db.Set(k, v)
			if err != nil {
				t.Fatal(err)
			}
		}

		for k, v := range updates {
			err = db.Set(k, v)
			if err != nil {
				t.Fatal(err)
			}

			delete(oldRecords, k)
		}

		for k, v := range updates {
			value, err := db.Get(k)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, v, value)
		}

		for k, v := range oldRecords {
			value, err := db.Get(k)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, v, value)
		}
	})

	t.Run("GetOldKeyShouldReturnValueForKeyInStore", func(t *testing.T) {
		db, err := connectToTestDb(dbPath, maxFileSizeKB, vacuumIntervalSec)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = db.Close()
			_ = internal.ClearDummyFileDataInDb(dbPath)
		}()

		value, err := db.Get("cow")
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "500 months", value)
	})

	t.Run("GetSameOldKeyAgainShouldGetValueFromMemoryCache", func(t *testing.T) {
		key, expectedValue := "cow", "500 months"

		db, err := connectToTestDb(dbPath, maxFileSizeKB, vacuumIntervalSec)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = db.Close()
			_ = internal.ClearDummyFileDataInDb(dbPath)
		}()

		_, err = db.Get(key)
		if err != nil {
			t.Fatal(err)
		}

		err = internal.ClearDummyFileDataInDb(dbPath)
		if err != nil {
			t.Fatal(err)
		}

		value, err := db.Get(key)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, expectedValue, value)
	})

	t.Run("GetNewlyInsertedKeyShouldGetValueFromMemoryMemtable", func(t *testing.T) {
		key, value := "hello", "world"
		db, err := connectToTestDb(dbPath, maxFileSizeKB, vacuumIntervalSec)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = db.Close()
			_ = internal.ClearDummyFileDataInDb(dbPath)
		}()

		err = db.Set(key, value)
		if err != nil {
			t.Fatal(err)
		}

		err = internal.ClearDummyFileDataInDb(dbPath)
		if err != nil {
			t.Fatal(err)
		}

		valueInDb, err := db.Get(key)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, value, valueInDb)
	})

	t.Run("DeleteShouldDeleteTheKeyValuePairFromStore", func(t *testing.T) {
		oldRecords := make(map[string]string, len(testRecords))
		for k, v := range testRecords {
			oldRecords[k] = v
		}
		keysToDelete := []string{"hey", "salut"}

		db, err := connectToTestDb(dbPath, maxFileSizeKB, vacuumIntervalSec)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = db.Close()
			_ = internal.ClearDummyFileDataInDb(dbPath)
		}()

		for k, v := range oldRecords {
			err = db.Set(k, v)
			if err != nil {
				t.Fatal(err)
			}
		}

		for _, key := range keysToDelete {
			err = db.Delete(key)
			if err != nil {
				t.Fatal(err)
			}

			delete(oldRecords, key)
		}

		for _, key := range keysToDelete {
			_, err = db.Get(key)
			assert.True(t, errors.Is(internal.ErrNotFound, err))
		}

		for k, v := range oldRecords {
			value, err := db.Get(k)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, v, value)
		}
	})

	t.Run("ClearShouldDeleteAllKeysFromStore", func(t *testing.T) {
		db, err := connectToTestDb(dbPath, maxFileSizeKB, vacuumIntervalSec)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = db.Close()
			_ = internal.ClearDummyFileDataInDb(dbPath)
		}()

		for k, v := range testRecords {
			err = db.Set(k, v)
			if err != nil {
				t.Fatal(err)
			}
		}

		err = db.Clear()
		if err != nil {
			t.Fatal(err)
		}

		for k := range testRecords {
			_, err = db.Get(k)
			assert.True(t, errors.Is(internal.ErrNotFound, err))
		}
	})

	t.Run("VacuumTaskRunsAtTheGivenInterval", func(t *testing.T) {
		keyToDelete := "salut"
		db, err := connectToTestDb(dbPath, maxFileSizeKB*80, vacuumIntervalSec)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = db.Close()
			_ = internal.ClearDummyFileDataInDb(dbPath)
		}()

		for k, v := range testRecords {
			err = db.Set(k, v)
			if err != nil {
				t.Fatal(err)
			}
		}
		err = db.Delete(keyToDelete)
		if err != nil {
			t.Fatal(err)
		}

		idxFileContents, err := internal.ReadFilesWithExtension(dbPath, "idx")
		if err != nil {
			t.Fatal(err)
		}
		delFileContents, err := internal.ReadFilesWithExtension(dbPath, "del")
		if err != nil {
			t.Fatal(err)
		}
		logFileContents, err := internal.ReadFilesWithExtension(dbPath, "log")
		if err != nil {
			t.Fatal(err)
		}

		<-time.After(time.Second * time.Duration(vacuumIntervalSec))

		idxFileContentsAfterVacuum, err := internal.ReadFilesWithExtension(dbPath, "idx")
		if err != nil {
			t.Fatal(err)
		}
		delFileContentsAfterVacuum, err := internal.ReadFilesWithExtension(dbPath, "del")
		if err != nil {
			t.Fatal(err)
		}
		logFileContentsAfterVacuum, err := internal.ReadFilesWithExtension(dbPath, "log")
		if err != nil {
			t.Fatal(err)
		}

		assert.NotContains(t, idxFileContents[0], keyToDelete)
		assert.Contains(t, delFileContents[0], keyToDelete)
		assert.Contains(t, logFileContents[0], keyToDelete)
		assert.NotContains(t, idxFileContentsAfterVacuum[0], keyToDelete)
		assert.NotContains(t, delFileContentsAfterVacuum[0], keyToDelete)
		assert.NotContains(t, logFileContentsAfterVacuum[0], keyToDelete)
	})

	t.Run("LogFileShouldBeTurnedToCkyFileAfterItExceedsTheMaxFileSizeKB", func(t *testing.T) {
		var preRollData []map[string]string
		postRollData := map[string]string{
			"hey": "English",
			"hi":  "English",
		}

		err := internal.ClearDummyFileDataInDb(dbPath)
		if err != nil {
			t.Fatal(err)
		}

		db, err := Connect(dbPath, maxFileSizeKB, vacuumIntervalSec)
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			_ = db.Close()
			_ = internal.ClearDummyFileDataInDb(dbPath)
		}()

		for i := 0; i < 3; i++ {
			data := map[string]string{}

			for k, v := range testRecords {
				key := fmt.Sprintf("%s-%d", k, i)
				data[key] = v

				err := db.Set(key, v)
				if err != nil {
					t.Fatal(err)
				}
			}

			preRollData = append(preRollData, data)
		}

		for k, v := range postRollData {
			err = db.Set(k, v)
			if err != nil {
				t.Fatal(err)
			}
		}

		ckyFileContentsAfterRoll, err := internal.ReadFilesWithExtension(dbPath, "cky")
		if err != nil {
			t.Fatal(err)
		}
		logFileContentsAfterRoll, err := internal.ReadFilesWithExtension(dbPath, "log")
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(ckyFileContentsAfterRoll)

		assert.Equal(t, len(preRollData), len(ckyFileContentsAfterRoll))
		for i, keySet := range preRollData {
			for k, v := range keySet {
				keyValuePair := fmt.Sprintf("%s%s%s", k, internal.KeyValueSeparator, v)
				assert.Contains(t, ckyFileContentsAfterRoll[i], keyValuePair)
			}
		}

		for k, v := range postRollData {
			keyValuePair := fmt.Sprintf("%s%s%s", k, internal.KeyValueSeparator, v)
			assert.Contains(t, logFileContentsAfterRoll[0], keyValuePair)
		}
	})
}

// connectToTestDb opens the db at the given path after
// clearing out old data
func connectToTestDb(dbPath string, maxFileSizeKB float64, vacuumIntervalSec float64) (*Ckydb, error) {
	err := internal.ClearDummyFileDataInDb(dbPath)
	if err != nil {
		return nil, err
	}

	err = internal.AddDummyFileDataInDb(dbPath)
	if err != nil {
		return nil, err
	}

	return Connect(dbPath, maxFileSizeKB, vacuumIntervalSec)
}
