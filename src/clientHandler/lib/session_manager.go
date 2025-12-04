package clientHandler

import (
	logger "common/logger"
	"sync"
	"time"

	"github.com/op/go-logging"
)

const (
	SESSION_TIMEOUT = 10 * time.Minute
)

type SessionManager struct {
	activeSessions       map[string]bool
	disconnectedSessions map[string]time.Time
	waitingSessions      map[string]chan bool
	mutex                sync.Mutex
	log                  *logging.Logger
}

func NewSessionManager() *SessionManager {
	return &SessionManager{
		activeSessions:       make(map[string]bool),
		disconnectedSessions: make(map[string]time.Time),
		waitingSessions:      make(map[string]chan bool),
		mutex:                sync.Mutex{},
		log:                  logger.GetLoggerWithPrefix("[SESSION_MGR]"),
	}
}

func (sm *SessionManager) RegisterSession(clientId string) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	sm.activeSessions[clientId] = true
	delete(sm.disconnectedSessions, clientId)
	sm.log.Infof("Registered session for client %s", clientId)
}

func (sm *SessionManager) UnregisterSession(clientId string) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	if _, ok := sm.activeSessions[clientId]; ok {
		delete(sm.activeSessions, clientId)
		sm.disconnectedSessions[clientId] = time.Now()
		sm.log.Infof("Unregistered session for client %s. Added to disconnected list.", clientId)
	}
}

func (sm *SessionManager) ValidateSession(clientId string) bool {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	// Check if already active
	if _, ok := sm.activeSessions[clientId]; ok {
		sm.log.Warningf("Client %s attempted to reconnect but is already marked active.", clientId)
		return true
	}

	disconnectTime, ok := sm.disconnectedSessions[clientId]
	if !ok {
		sm.log.Infof("Client %s not found in disconnected sessions.", clientId)
		return false
	}

	if time.Since(disconnectTime) > SESSION_TIMEOUT {
		sm.log.Infof("Client %s session expired (disconnected at %v).", clientId, disconnectTime)
		delete(sm.disconnectedSessions, clientId)
		return false
	}

	sm.log.Infof("Client %s session valid (disconnected at %v).", clientId, disconnectTime)
	return true
}

func (sm *SessionManager) IsSessionActive(clientId string) bool {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	_, ok := sm.activeSessions[clientId]
	return ok
}

// WaitForReconnection blocks until the client reconnects or the timeout expires.
// Returns true if reconnected, false if timed out.
func (sm *SessionManager) WaitForReconnection(clientId string, timeout time.Duration) bool {
	sm.mutex.Lock()
	// Create a channel for this client if it doesn't exist
	if _, exists := sm.waitingSessions[clientId]; !exists {
		sm.waitingSessions[clientId] = make(chan bool, 1)
	}
	waitChan := sm.waitingSessions[clientId]
	sm.mutex.Unlock()

	sm.log.Infof("Waiting for reconnection of client %s for %v", clientId, timeout)

	select {
	case <-waitChan:
		sm.log.Infof("Client %s reconnected successfully.", clientId)
		return true
	case <-time.After(timeout):
		sm.log.Infof("Timeout waiting for reconnection of client %s.", clientId)

		sm.mutex.Lock()
		delete(sm.waitingSessions, clientId)
		sm.mutex.Unlock()

		return false
	}
}

// SignalReconnection notifies a waiting session that a reconnection has occurred.
func (sm *SessionManager) SignalReconnection(clientId string) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	if ch, ok := sm.waitingSessions[clientId]; ok {
		// Non-blocking send to avoid deadlocks if no one is listening (though WaitForReconnection should be)
		select {
		case ch <- true:
			sm.log.Infof("Signaled reconnection for client %s", clientId)
		default:
			sm.log.Warningf("Failed to signal reconnection for client %s (channel full?)", clientId)
		}
		// Clean up the map entry as the signal is sent
		delete(sm.waitingSessions, clientId)
	} else {
		sm.log.Warningf("No waiting session found for client %s to signal.", clientId)
	}
}
