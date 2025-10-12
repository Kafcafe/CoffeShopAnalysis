package filters

type ClientStats struct {
	processed map[DataType]int
	emitted   map[DataType]int
	eof       map[DataType]int
	eofChan   map[DataType]chan int
}

func NewClientStats() *ClientStats {
	return &ClientStats{
		processed: make(map[DataType]int),
		emitted:   make(map[DataType]int),
		eof:       make(map[DataType]int),
		eofChan:   make(map[DataType]chan int),
	}
}

func (cs *ClientStats) AddProcessed(dataType DataType) {
	if _, exists := cs.processed[dataType]; !exists {
		cs.processed[dataType] = 0
	}
	cs.processed[dataType] += 1
}

func (cs *ClientStats) AddEmitted(dataType DataType) {
	if _, exists := cs.emitted[dataType]; !exists {
		cs.emitted[dataType] = 0
	}
	cs.emitted[dataType] += 1
}

func (cs *ClientStats) SetEof(dataType DataType, count int) {
	cs.eof[dataType] = count
}

func (cs *ClientStats) GetProcessed(dataType DataType) int {
	return cs.processed[dataType]
}

func (cs *ClientStats) GetEmitted(dataType DataType) int {
	return cs.emitted[dataType]
}

func (cs *ClientStats) GetEof(dataType DataType) (int, bool) {
	val, exists := cs.eof[dataType]
	return val, exists
}

func (cs *ClientStats) SendEofChan(dataType DataType) {
	if _, exists := cs.eofChan[dataType]; !exists {
		cs.eofChan[dataType] = make(chan int, 1)
	}
	cs.eofChan[dataType] <- 1
}

func (cs *ClientStats) WaitForEofChan(dataType DataType) {
	if _, exists := cs.eofChan[dataType]; !exists {
		cs.eofChan[dataType] = make(chan int, 1)
	}
	<-cs.eofChan[dataType]
}
