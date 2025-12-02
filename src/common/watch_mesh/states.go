package watch_mesh

import (
	"fmt"
	"sync"
	"time"
)

// CurrentNodeStatus represents the state of the current node
type CurrentNodeStatus int

const (
	StatusAlive CurrentNodeStatus = iota
	StatusElectionStarter
	StatusCoordinatorCandidate
	StatusLeader
)

func (s CurrentNodeStatus) String() string {
	switch s {
	case StatusAlive:
		return "Alive"
	case StatusElectionStarter:
		return "ElectionStarter"
	case StatusCoordinatorCandidate:
		return "CoordinatorCandidate"
	case StatusLeader:
		return "Leader"
	default:
		return fmt.Sprintf("Unknown(%d)", s)
	}
}

// PeerStatus represents the state of a peer node
type PeerStatus int

const (
	PeerStatusAlive PeerStatus = iota
	PeerStatusDead
	PeerStatusLeader
	PeerStatusResurrecting
)

func (s PeerStatus) String() string {
	switch s {
	case PeerStatusAlive:
		return "Alive"
	case PeerStatusDead:
		return "Dead"
	case PeerStatusLeader:
		return "Leader"
	case PeerStatusResurrecting:
		return "Resurrecting"
	default:
		return fmt.Sprintf("Unknown(%d)", s)
	}
}

// MyStateMachine manages the state of the current node
type MyStateMachine struct {
	state CurrentNodeStatus
	mu    sync.Mutex
}

func NewMyStateMachine() *MyStateMachine {
	return &MyStateMachine{
		state: StatusAlive,
	}
}

func (sm *MyStateMachine) Get() CurrentNodeStatus {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.state
}

func (sm *MyStateMachine) Set(newState CurrentNodeStatus) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.state = newState
}

// PeerStateMachine manages the state of a peer node
type PeerStateMachine struct {
	state               PeerStatus
	lastSeen            time.Time
	resurrectingChecks  int
	mu                  sync.Mutex
	lastSeenInitialized bool
}

func NewPeerStateMachine() *PeerStateMachine {
	return &PeerStateMachine{
		state:               PeerStatusAlive,
		lastSeen:            time.Time{},
		lastSeenInitialized: false,
		resurrectingChecks:  0,
	}
}

func (sm *PeerStateMachine) Get() PeerStatus {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.state
}

func (sm *PeerStateMachine) Set(newState PeerStatus) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.state = newState
}

func (sm *PeerStateMachine) UpdateLastSeen() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.lastSeen = time.Now()
	sm.lastSeenInitialized = true
}

func (sm *PeerStateMachine) GetLastSeen() time.Time {
	sm.mu.Lock()

	if !sm.lastSeenInitialized {
		sm.lastSeen = time.Now()
		sm.lastSeenInitialized = true
	}

	defer sm.mu.Unlock()
	return sm.lastSeen
}

func (sm *PeerStateMachine) IncrementResurrectingChecks() int {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.resurrectingChecks++
	return sm.resurrectingChecks
}

func (sm *PeerStateMachine) ResetResurrectingChecks() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.resurrectingChecks = 0
}
