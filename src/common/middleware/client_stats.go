package middleware

import "sync"

type DataType = string

type ClientStats struct {
	processed map[DataType]int
	emitted   map[DataType]int
	eof       map[DataType]int
	eofChan   map[DataType]chan int
	mutex     sync.Mutex
}

func NewClientStats() *ClientStats {
	return &ClientStats{
		processed: make(map[DataType]int),
		emitted:   make(map[DataType]int),
		mutex:     sync.Mutex{},
		// deprecated
		eof:     make(map[DataType]int),
		eofChan: make(map[DataType]chan int),
	}
}

func (cs *ClientStats) ensureDatatypeExists(dataType DataType) {
	if _, exists := cs.processed[dataType]; !exists {
		cs.processed[dataType] = 0
	}
	if _, exists := cs.emitted[dataType]; !exists {
		cs.emitted[dataType] = 0
	}
	if _, exists := cs.eof[dataType]; !exists {
		cs.eof[dataType] = 0
	}
	if _, exists := cs.eofChan[dataType]; !exists {
		cs.eofChan[dataType] = make(chan int, 1)
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

// Down from here is deprecated

// TODO: Remove these individual methods if not needed
func (cs *ClientStats) AddProcessed(dataType DataType) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	if _, exists := cs.processed[dataType]; !exists {
		cs.processed[dataType] = 0
	}
	cs.processed[dataType] += 1
}

func (cs *ClientStats) AddEmitted(dataType DataType) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	if _, exists := cs.emitted[dataType]; !exists {
		cs.emitted[dataType] = 0
	}
	cs.emitted[dataType] += 1
}

// TODO: Check if needed
func (cs *ClientStats) SetEof(dataType DataType, count int) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	cs.ensureDatatypeExists(dataType)
	cs.eof[dataType] = count
}

// TODO: Remove these individual methods if not needed
func (cs *ClientStats) GetProcessed(dataType DataType) int {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	cs.ensureDatatypeExists(dataType)
	return cs.processed[dataType]
}

func (cs *ClientStats) GetEmitted(dataType DataType) int {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	cs.ensureDatatypeExists(dataType)
	return cs.emitted[dataType]
}

// TODO: Check if needed
func (cs *ClientStats) GetEof(dataType DataType) (int, bool) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	cs.ensureDatatypeExists(dataType)
	val, exists := cs.eof[dataType]
	return val, exists
}

// Maybe not needed
func (cs *ClientStats) SendEofChan(dataType DataType) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	cs.ensureDatatypeExists(dataType)
	cs.eofChan[dataType] <- 1
}

func (cs *ClientStats) WaitForEofChan(dataType DataType) {
	cs.mutex.Lock()
	cs.ensureDatatypeExists(dataType)
	cs.mutex.Unlock()
	<-cs.eofChan[dataType]
}
