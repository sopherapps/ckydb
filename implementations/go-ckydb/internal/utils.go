package internal

// ClearDummyFileDataInDb clears the files in the given database folder
func ClearDummyFileDataInDb(dbPath string) error {
	return nil
}

// AddDummyFileDataInDb adds dummy file data in the given database folder
// This is to be called before Connect() or Open() [for controllers] or Load() [for store]
func AddDummyFileDataInDb(dbPath string) error {
	return nil
}

// ReadFilesWithExtension reads all content in the files with the given extension 'ext' e.g. 'log'
// in the folder path
func ReadFilesWithExtension(folderPath string, ext string) ([]string, error) {
	return nil, nil
}
