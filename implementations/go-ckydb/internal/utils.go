package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var dummyDataFileMap = map[string]string{
	"1655375120328185000.cky": "1655375120328185000-cow><?&(^#500 months$%#@*&^&1655375120328185100-dog><?&(^#23 months$%#@*&^&",
	"1655375120328186000.cky": "1655375171402014000-bar><?&(^#foo$%#@*&^&",
	"1655375171402014000.log": "1655404770518678-goat><?&(^#678 months$%#@*&^&1655404670510698-hen><?&(^#567 months$%#@*&^&1655404770534578-pig><?&(^#70 months$%#@*&^&1655403775538278-fish><?&(^#8990 months$%#@*&^&1655403795838278-foo><?&(^#890 months$%#@*&^&",
	"delete.del":              "1655403795838278-foo$%#@*&^&1655375171402014000-bar$%#@*&^&",
	"index.idx":               "cow><?&(^#1655375120328185000-cow$%#@*&^&dog><?&(^#1655375120328185100-dog$%#@*&^&goat><?&(^#1655404770518678-goat$%#@*&^&hen><?&(^#1655404670510698-hen$%#@*&^&pig><?&(^#1655404770534578-pig$%#@*&^&fish><?&(^#1655403775538278-fish$%#@*&^&",
}

// ClearDummyFileDataInDb clears the files in the given database folder
func ClearDummyFileDataInDb(dbPath string) error {
	return os.RemoveAll(dbPath)
}

// AddDummyFileDataInDb adds dummy file data in the given database folder
// This is to be called before Connect() or Open() [for controllers] or Load() [for store]
func AddDummyFileDataInDb(dbPath string) error {
	fileMode := os.FileMode(0777)
	err := os.MkdirAll(dbPath, fileMode)
	if err != nil {
		return err
	}

	for filename, content := range dummyDataFileMap {
		err = os.WriteFile(filepath.Join(dbPath, filename), []byte(content), fileMode)
		if err != nil {
			return err
		}
	}

	return nil
}

// ReadFilesWithExtension reads all content in the files with the given extension 'ext' e.g. 'log'
// in the folder path
func ReadFilesWithExtension(folderPath string, ext string) ([]string, error) {
	files, err := os.ReadDir(folderPath)
	if err != nil {
		return nil, err
	}

	var contents []string
	for _, file := range files {
		filename := file.Name()
		if strings.HasSuffix(filename, ext) {
			filePath := filepath.Join(folderPath, filename)
			data, err := os.ReadFile(filePath)
			if err != nil {
				return nil, err
			}

			contents = append(contents, string(data))
		}
	}

	return contents, nil
}

// GetFileOrFolderNamesInFolder returns a list of the names of the files or folders
// in the given folder
func GetFileOrFolderNamesInFolder(folderPath string) ([]string, error) {
	entries, err := os.ReadDir(folderPath)
	if err != nil {
		return nil, err
	}

	filenames := make([]string, len(entries))
	for i, file := range entries {
		filenames[i] = file.Name()
	}

	return filenames, nil
}

// CreateFileIfNotExist creates a file if it does not exist
func CreateFileIfNotExist(filePath string) error {
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}

	return f.Close()
}

// ExtractKeyValuesFromByteArray extracts a map of keys and values from a byte array
func ExtractKeyValuesFromByteArray(data []byte) (map[string]string, error) {
	kvPairStrings, err := ExtractTokensFromByteArray(data)
	if err != nil {
		return nil, err
	}
	result := make(map[string]string, len(kvPairStrings))

	for _, kv := range kvPairStrings {
		kvParts := strings.Split(kv, KeyValueSeparator)
		if len(kvParts) != 2 {
			return nil, ErrCorruptedData
		}

		result[kvParts[0]] = kvParts[1]
	}

	return result, nil
}

// ExtractTokensFromByteArray extracts tokens from a byte array
func ExtractTokensFromByteArray(data []byte) ([]string, error) {
	dataAsStr := strings.TrimRight(string(data), TokenSeparator)
	if dataAsStr == "" {
		return []string{}, nil
	}

	tokens := strings.Split(dataAsStr, TokenSeparator)
	return tokens, nil
}

// DeleteKeyValuesFromFile deletes the key values corresponding to the keysToDelete
// if those keys exist in that file
func DeleteKeyValuesFromFile(path string, keysToDelete []string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	kvPairStrings, err := ExtractTokensFromByteArray(data)
	if err != nil {
		return err
	}

	prefixesToDelete := make([]string, len(keysToDelete))
	for i, key := range keysToDelete {
		prefixesToDelete[i] = fmt.Sprintf("%s%s", key, KeyValueSeparator)
	}

	content := ""
	for _, pairString := range kvPairStrings {
		if hasAnyOfPrefixes(pairString, prefixesToDelete) {
			continue
		}

		content = fmt.Sprintf("%s%s%s", content, pairString, TokenSeparator)
	}

	err = os.WriteFile(path, []byte(content), 0666)
	if err != nil {
		return err
	}

	return nil
}

// ReadFileToString reads the contents at the given path into a string
func ReadFileToString(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// hasAnyOfPrefixes checks if the string str has any of the prefixes
func hasAnyOfPrefixes(str string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(str, prefix) {
			return true
		}
	}
	return false
}
