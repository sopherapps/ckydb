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

// IsInRange checks if the passed key is in the range between the start
// and end of this Cache
func (c *Cache) IsInRange(key string) bool {
	return c.start <= key && key <= c.end
}

// Update updates the data of the givne cache with the new key value pair
func (c *Cache) Update(key string, value string) {
	c.data[key] = value
}

// Remove removes the key-value pair corresponding to the given key from the data
func (c *Cache) Remove(key string) {
	delete(c.data, key)
}
