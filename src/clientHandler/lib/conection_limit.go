package clientHandler

import "sync"

type ConnectionLimit struct {
	MaxConnections     int
	CurrentConnections int
	isFree             *sync.Cond
	mtx                sync.Mutex
}

func NewConnectionLimit(maxConnections int) *ConnectionLimit {
	return &ConnectionLimit{
		MaxConnections:     maxConnections,
		CurrentConnections: 0,
		mtx:                sync.Mutex{},
		isFree:             sync.NewCond(&sync.Mutex{}),
	}
}

func (cl *ConnectionLimit) Wait() {
	if cl.CurrentConnections == cl.MaxConnections {
		cl.isFree.Wait()
	}

	cl.mtx.Lock()
	cl.CurrentConnections++
	cl.mtx.Unlock()
}

func (cl *ConnectionLimit) Signal() {
	if cl.CurrentConnections == cl.MaxConnections {
		cl.isFree.Signal()
	}

	cl.mtx.Lock()
	cl.CurrentConnections--
	cl.mtx.Unlock()
}

func (cl *ConnectionLimit) Shutdown() {
	cl.mtx.Unlock()
	cl.isFree.Signal()
}
