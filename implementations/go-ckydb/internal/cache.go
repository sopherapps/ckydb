package internal

// Cache contains the cached data as a map, plus its
// start and end timestamps to ease checking for any key
type Cache struct {
	data  map[string]string
	start string
	end   string
}

// NewCache creates a new Cache instance
func NewCache(data map[string]string, start string, end string) *Cache {
	return &Cache{data: data, start: start, end: end}
}
