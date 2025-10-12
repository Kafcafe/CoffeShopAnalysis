package clientHandler

import "sync"

type ConnectionLimit struct {
	MaxConnections     int
	CurrentConnections int
	mtx                sync.Mutex
}

func NewConnectionLimit(maxConnections int) *ConnectionLimit {
	return &ConnectionLimit{
		MaxConnections:     maxConnections,
		CurrentConnections: 0,
		mtx:                sync.Mutex{},
	}
}

func (cl *ConnectionLimit) Wait() {
	if cl.CurrentConnections == cl.MaxConnections {
		cl.mtx.Lock()
	}

	cl.CurrentConnections++
}

func (cl *ConnectionLimit) Signal() {
	cl.CurrentConnections--
	if cl.CurrentConnections < cl.MaxConnections {
		cl.mtx.Unlock()
	}
}
