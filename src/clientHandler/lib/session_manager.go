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
	mu                   sync.Mutex
	log                  *logging.Logger
}

func NewSessionManager() *SessionManager {
	return &SessionManager{
		activeSessions:       make(map[string]bool),
		disconnectedSessions: make(map[string]time.Time),
		log:                  logger.GetLoggerWithPrefix("[SESSION_MGR]"),
	}
}

func (sm *SessionManager) RegisterSession(clientId string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.activeSessions[clientId] = true
	delete(sm.disconnectedSessions, clientId)
	sm.log.Infof("Registered session for client %s", clientId)
}

func (sm *SessionManager) UnregisterSession(clientId string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, ok := sm.activeSessions[clientId]; ok {
		delete(sm.activeSessions, clientId)
		sm.disconnectedSessions[clientId] = time.Now()
		sm.log.Infof("Unregistered session for client %s. Added to disconnected list.", clientId)
	}
}

func (sm *SessionManager) ValidateSession(clientId string) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check if already active (should not happen in normal flow, but good for safety)
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
