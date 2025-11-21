package middleware

import "sync"

type DataType = string

type ClientStats struct {
	processed map[DataType]int
	emitted   map[DataType]int
	mutex     sync.Mutex
}

func NewClientStats() *ClientStats {
	return &ClientStats{
		processed: make(map[DataType]int),
		emitted:   make(map[DataType]int),
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

func (cs *ClientStats) Add(dataType DataType, processed, emitted bool) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	cs.ensureDatatypeExists(dataType)
	if processed {
		cs.processed[dataType] += 1
	}
	if emitted {
		cs.emitted[dataType] += 1
	}
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
