package middleware

type Cache struct {
	capacity int
	buffer   []string
	next     int
	items    map[string]bool
}

func NewCache(capacity int) *Cache {
	return &Cache{
		capacity: capacity,
		buffer:   make([]string, capacity),
		next:     0,
		items:    make(map[string]bool),
	}
}

func (c *Cache) Contains(key string) bool {
	_, exists := c.items[key]
	return exists
}

func (c *Cache) Add(key string) {
	if _, exists := c.items[key]; exists {
		return
	}

	// delete oldest when full
	if len(c.items) == c.capacity {
		oldest := c.buffer[c.next]
		delete(c.items, oldest)
	}

	// add new item
	c.buffer[c.next] = key
	c.items[key] = true
	c.next = (c.next + 1) % c.capacity
}