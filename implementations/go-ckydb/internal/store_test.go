package internal

import "testing"

func TestStore(t *testing.T) {
	t.Run("LoadShouldUpdateMemoryPropsFromDataOnDisk", func(t *testing.T) {

	})

	t.Run("SetNewKeyShouldAddKeyValueToMemtableAndIndexAndLogFile", func(t *testing.T) {

	})

	t.Run("SetOldKeyShouldUpdateKeyValueInCacheAndDataFile", func(t *testing.T) {

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

	})
}
