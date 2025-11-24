package middleware

import (
	"encoding/json"
	"sync"
)

type DataType = string

type ClientStats struct {
	processed map[DataType]int
	emitted   map[DataType]int
	cache     *Cache
	mutex     sync.Mutex
}

func NewClientStats(cacheCapacity int) *ClientStats {
	return &ClientStats{
		processed: make(map[DataType]int),
		emitted:   make(map[DataType]int),
		cache:     NewCache(cacheCapacity),
		mutex:     sync.Mutex{},
	}
}

func (cs *ClientStats) ensureDatatypeExists(dataType DataType) {
	if _, exists := cs.processed[dataType]; !exists {
		cs.processed[dataType] = 0
	}
	if _, exists := cs.emitted[dataType]; !exists {
		cs.emitted[dataType] = 0
	}
}

func (cs *ClientStats) Add(dataType DataType, messageId string, processed, emitted bool) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	cs.ensureDatatypeExists(dataType)
	if processed {
		cs.processed[dataType] += 1
	}
	if emitted {
		cs.emitted[dataType] += 1
	}
	cs.cache.Add(messageId)
}

func (cs *ClientStats) WasMessageProcessed(messageId string) bool {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	return cs.cache.Contains(messageId)
}

func (cs *ClientStats) Remove(dataType DataType, processed, emitted int) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	cs.ensureDatatypeExists(dataType)
	cs.processed[dataType] -= processed
	cs.emitted[dataType] -= emitted
}

func (cs *ClientStats) GetStats(dataType DataType) (processed int, emitted int) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	cs.ensureDatatypeExists(dataType)
	return cs.processed[dataType], cs.emitted[dataType]
}

func (cs *ClientStats) Clear(dataType DataType) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	cs.ensureDatatypeExists(dataType)
	cs.processed[dataType] = 0
	cs.emitted[dataType] = 0
}

func (cs *ClientStats) SetCount(dataType DataType, emitted int) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	cs.ensureDatatypeExists(dataType)
	cs.processed[dataType] = emitted
}

func (cs *ClientStats) toDTO() clientStatsDTO {
	return clientStatsDTO{
		Processed: cs.processed,
		Emitted:   cs.emitted,
		Cache:     cs.cache,
	}
}

func (cs *ClientStats) fromDTO(dto clientStatsDTO) {
	cs.processed = dto.Processed
	cs.emitted = dto.Emitted
	cs.cache = dto.Cache
}

type clientStatsDTO struct {
	Processed map[DataType]int
	Emitted   map[DataType]int
	Cache     *Cache
}

func (cs *ClientStats) MarshalJSON() ([]byte, error) {
	return json.Marshal(cs.toDTO())
}

func (cs *ClientStats) UnmarshalJSON(data []byte) error {
	var dto clientStatsDTO
	if err := json.Unmarshal(data, &dto); err != nil {
		return err
	}
	cs.fromDTO(dto)
	return nil
}
