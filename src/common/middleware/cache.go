package middleware

import "encoding/json"

const DEFAULT_CACHE_CAPACITY = 1000

type Cache struct {
	capacity int
	buffer   []string
	next     int
	items    map[string]bool
}

func NewCache(capacity int) *Cache {
	if capacity <= 0 {
		return nil
	}
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
	if c.capacity == 0 || key == "" || c.Contains(key) {
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

func (c *Cache) toDTO() cacheDTO {
	return cacheDTO{
		Capacity: c.capacity,
		Buffer:   c.buffer,
		Next:     c.next,
	}
}

func (c *Cache) fromDTO(dto cacheDTO) {
	c.capacity = dto.Capacity
	c.buffer = dto.Buffer
	c.next = dto.Next
	c.items = make(map[string]bool)
	for _, item := range c.buffer {
		c.items[item] = true
	}
}

type cacheDTO struct {
	Capacity int      `json:"capacity"`
	Buffer   []string `json:"buffer"`
	Next     int      `json:"next"`
}

func (c *Cache) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.toDTO())
}

func (c *Cache) UnmarshalJSON(data []byte) error {
	var dto cacheDTO
	if err := json.Unmarshal(data, &dto); err != nil {
		return err
	}
	c.fromDTO(dto)
	return nil
}
