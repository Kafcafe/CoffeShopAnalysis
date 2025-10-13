package clientHandler

import (
	"sync"

	"github.com/op/go-logging"
)

type ConnectionLimit struct {
	MaxConnections     int
	CurrentConnections int
	mu                 sync.Mutex
	cond               *sync.Cond
	closed             bool
	log                *logging.Logger
}

func NewConnectionLimit(maxConnections int) *ConnectionLimit {
	cl := &ConnectionLimit{
		MaxConnections:     maxConnections,
		CurrentConnections: 0,
		log:                logging.MustGetLogger("LIMIT"),
	}
	cl.cond = sync.NewCond(&cl.mu)
	return cl
}

// Wait blocks until there's capacity or the limit is shutdown.
// It increments the CurrentConnections before returning.
func (cl *ConnectionLimit) Wait() {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	cl.log.Infof("action: about to wait | current_connections: %d | max_connections: %d", cl.CurrentConnections, cl.MaxConnections)
	for !cl.closed && cl.CurrentConnections >= cl.MaxConnections {
		cl.log.Infof("action: waiting | current_connections: %d | max_connections: %d", cl.CurrentConnections, cl.MaxConnections)
		cl.cond.Wait()
	}
	cl.log.Infof("action: proceeding | current_connections: %d | max_connections: %d", cl.CurrentConnections, cl.MaxConnections)

	if cl.closed {
		return
	}

	cl.CurrentConnections++
}

// Signal releases one slot (if any) and wakes one waiter.
func (cl *ConnectionLimit) Signal() {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	if cl.CurrentConnections > 0 {
		cl.CurrentConnections--
	}

	cl.log.Infof("action: signaled | current_connections: %d | max_connections: %d", cl.CurrentConnections, cl.MaxConnections)
	cl.cond.Signal()
}

// Shutdown wakes all waiters and prevents new Wait calls from blocking.
func (cl *ConnectionLimit) Shutdown() {
	cl.mu.Lock()
	cl.closed = true
	cl.cond.Broadcast()
	cl.mu.Unlock()
}
