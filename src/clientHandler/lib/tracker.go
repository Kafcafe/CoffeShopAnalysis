package clientHandler

import (
	"common/middleware"
	"sync"

	logger "common/logger"

	"github.com/op/go-logging"
)

type Tracker struct {
	track map[string]map[int]int
	mtx   sync.Mutex
	log   *logging.Logger
}

func NewTracker() *Tracker {
	return &Tracker{
		track: make(map[string]map[int]int),
		mtx:   sync.Mutex{},
		log:   logger.GetLoggerWithPrefix("Tracker"),
	}
}

func (t *Tracker) Track(clientId string, query int, msg *middleware.Message) {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	if _, exists := t.track[clientId]; !exists {
		t.track[clientId] = make(map[int]int)
	}

	t.track[clientId][query] += 1
	t.log.Infof("Tracking state: %+v", t.track)
}

func (t *Tracker) CanSendEof(clientId string, query int, msg *middleware.Message) bool {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	if _, exists := t.track[clientId]; !exists {
		return true
	}

	if count, exists := t.track[clientId][query]; exists {
		result := count >= msg.TotalEmitted
		t.log.Infof("Can send EOF? %v (count: %d, totalEmitted: %d)", result, count, msg.TotalEmitted)
		t.log.Infof("Tracking state for query: %d, is ended: %v", query, result)
		t.log.Infof("Tracking state: %+v", t.track)
		t.log.Infof("Message state: %+v", msg)
		return result
	}

	return false
}

func (t *Tracker) Reset(clientId string) {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	delete(t.track, clientId)
}
