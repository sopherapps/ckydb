package ckydb

import (
	"errors"
	"testing"
	"time"

	"github.com/sopherapps/ckydb/implementations/go-ckydb/internal"
	"github.com/stretchr/testify/assert"
)

func TestCkydb(t *testing.T) {
	type testRecord struct {
		key   string
		value string
	}
	dbPath := "test_ckydb"
	vacuumIntervalInSec := 2

	t.Run("ConnectShouldCallOpen", func(t *testing.T) {
		db, err := connectToTestDb(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = db.Close()
		}()

		assert.Greater(t, len(db.tasks), 0)
	})

	t.Run("OpenShouldStartAllBackgroundTasks", func(t *testing.T) {
		db := Ckydb{dbPath: dbPath}

		err := db.Open()
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = db.Close()
		}()

		assert.Greater(t, len(db.tasks), 0)
	})

	t.Run("CloseShouldStopAllBackgroundTasks", func(t *testing.T) {
		db, err := connectToTestDb(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = db.Close()
		}()

		err = db.Close()
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, len(db.tasks), 0)
		assert.PanicsWithError(t, "closed", func() {
			db.controlChan <- struct{}{}
		})
	})

	t.Run("SetNewKeyShouldAddKeyValueToStore", func(t *testing.T) {
		testRecords := []testRecord{
			{key: "", value: ""},
		}

		err := internal.ClearDummyFileDataInDb(dbPath)
		if err != nil {
			t.Fatal(err)
		}

		db, err := Connect(dbPath, 6, 3)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = db.Close()
		}()

		for _, record := range testRecords {
			err = db.Set(record.key, record.value)
			if err != nil {
				t.Fatal(err)
			}
		}

		for _, record := range testRecords {
			value, err := db.Get(record.key)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, record.value, value)
		}
	})

	t.Run("SetOldKeyShouldUpdateOldKeyWithValue", func(t *testing.T) {
		oldRecords := map[string]testRecord{
			"": {key: "", value: ""},
		}
		updates := []testRecord{
			{key: "", value: ""},
		}

		err := internal.ClearDummyFileDataInDb(dbPath)
		if err != nil {
			t.Fatal(err)
		}

		db, err := Connect(dbPath, 6, 3)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = db.Close()
		}()

		for _, record := range oldRecords {
			err = db.Set(record.key, record.value)
			if err != nil {
				t.Fatal(err)
			}
		}

		for _, record := range updates {
			err = db.Set(record.key, record.value)
			if err != nil {
				t.Fatal(err)
			}

			delete(oldRecords, record.key)
		}

		for _, record := range updates {
			value, err := db.Get(record.key)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, record.value, value)
		}

		for _, record := range oldRecords {
			value, err := db.Get(record.key)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, record.value, value)
		}
	})

	t.Run("GetOldKeyShouldReturnValueForKeyInStore", func(t *testing.T) {
		db, err := connectToTestDb(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = db.Close()
		}()

		value, err := db.Get("foo")
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "bar", value)
	})

	t.Run("GetSameOldKeyAgainShouldGetValueFromMemoryCache", func(t *testing.T) {
		db, err := connectToTestDb(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = db.Close()
		}()

		err = internal.ClearDummyFileDataInDb(dbPath)
		if err != nil {
			t.Fatal(err)
		}

		value, err := db.Get("foo")
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "bar", value)
	})

	t.Run("GetNewlyInsertedKeyShouldGetValueFromMemoryMemtable", func(t *testing.T) {
		key, value := "hello", "world"
		db, err := connectToTestDb(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = db.Close()
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
		oldRecords := map[string]testRecord{
			"": {key: "", value: ""},
		}
		keysToDelete := []string{""}

		db, err := connectToTestDb(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = db.Close()
		}()

		for _, record := range oldRecords {
			err = db.Set(record.key, record.value)
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
			assert.True(t, errors.Is(ErrNotFound, err))
		}

		for _, record := range oldRecords {
			value, err := db.Get(record.key)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, record.value, value)
		}
	})

	t.Run("ClearShouldDeleteAllKeysFromStore", func(t *testing.T) {
		oldRecords := map[string]testRecord{
			"": {key: "", value: ""},
		}

		db, err := connectToTestDb(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = db.Close()
		}()

		for _, record := range oldRecords {
			err = db.Set(record.key, record.value)
			if err != nil {
				t.Fatal(err)
			}
		}

		err = db.Clear()
		if err != nil {
			t.Fatal(err)
		}

		for _, record := range oldRecords {
			_, err = db.Get(record.key)
			assert.True(t, errors.Is(ErrNotFound, err))
		}
	})

	t.Run("VacuumTaskRunsAtTheGivenInterval", func(t *testing.T) {
		expectedDelFileContents := []string{""}
		expectedLogFileContents := []string{""}
		expectedDataFilesContents := []string{""}
		expectedDelFileContentsAfterVacuum := []string{""}
		expectedLogFileContentsAfterVacuum := []string{""}
		expectedDataFilesContentsAfterVacuum := []string{""}

		db, err := connectToTestDb(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = db.Close()
		}()

		delFileContents, err := internal.ReadFilesWithExtension(dbPath, "del")
		if err != nil {
			t.Fatal(err)
		}
		logFileContents, err := internal.ReadFilesWithExtension(dbPath, "log")
		if err != nil {
			t.Fatal(err)
		}
		dataFilesContents, err := internal.ReadFilesWithExtension(dbPath, "cky")
		if err != nil {
			t.Fatal(err)
		}

		<-time.After(time.Second * time.Duration(vacuumIntervalInSec))

		delFileContentsAfterVacuum, err := internal.ReadFilesWithExtension(dbPath, "del")
		if err != nil {
			t.Fatal(err)
		}
		logFileContentsAfterVacuum, err := internal.ReadFilesWithExtension(dbPath, "log")
		if err != nil {
			t.Fatal(err)
		}
		dataFilesContentsAfterVacuum, err := internal.ReadFilesWithExtension(dbPath, "cky")
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, expectedDelFileContents, delFileContents)
		assert.Equal(t, expectedLogFileContents, logFileContents)
		assert.Equal(t, expectedDataFilesContents, dataFilesContents)
		assert.Equal(t, expectedDelFileContentsAfterVacuum, delFileContentsAfterVacuum)
		assert.Equal(t, expectedLogFileContentsAfterVacuum, logFileContentsAfterVacuum)
		assert.Equal(t, expectedDataFilesContentsAfterVacuum, dataFilesContentsAfterVacuum)
	})
}

// connectToTestDb opens the db at the given path after
// clearing out old data and adding in new test data
func connectToTestDb(dbPath string) (*Ckydb, error) {
	err := internal.ClearDummyFileDataInDb(dbPath)
	if err != nil {
		return nil, err
	}

	err = internal.AddDummyFileDataInDb(dbPath)
	if err != nil {
		return nil, err
	}

	return Connect(dbPath, 6, 3)
}
