package watch_mesh

import (
	"sync"
	"time"
)

// CurrentNodeStatus represents the state of the current node.
type CurrentNodeStatus int

const (
	// StatusAlive means the node is functioning normally.
	StatusAlive CurrentNodeStatus = iota
	// StatusElectionStarter means the node has initiated an election.
	StatusElectionStarter
	// StatusCoordinatorCandidate means the node is a candidate to become the coordinator.
	StatusCoordinatorCandidate
	// StatusLeader means the node is currently the leader of the mesh.
	StatusLeader
)

// PeerStatus represents the state of a peer node.
type PeerStatus int

const (
	// PeerStatusAlive means the peer is responsive and functioning.
	PeerStatusAlive PeerStatus = iota
	// PeerStatusDead means the peer is considered dead or unresponsive.
	PeerStatusDead
	// PeerStatusLeader means the peer is currently the leader.
	PeerStatusLeader
	// PeerStatusResurrecting means the peer is being checked for responsiveness after being marked dead.
	PeerStatusResurrecting
)

// MyStateMachine manages the state of the current node in a thread-safe manner.
type MyStateMachine struct {
	state CurrentNodeStatus
	mu    sync.Mutex
}

// NewMyStateMachine creates a new instance of MyStateMachine with initial state StatusAlive.
func NewMyStateMachine() *MyStateMachine {
	return &MyStateMachine{
		state: StatusAlive,
	}
}

// Get returns the current state of the node.
func (sm *MyStateMachine) Get() CurrentNodeStatus {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.state
}

// Set updates the state of the node to the new value.
func (sm *MyStateMachine) Set(newState CurrentNodeStatus) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.state = newState
}

// PeerStateMachine manages the state of a peer node in a thread-safe manner.
type PeerStateMachine struct {
	state               PeerStatus
	lastSeen            time.Time
	resurrectingChecks  int
	mu                  sync.Mutex
	lastSeenInitialized bool
}

// NewPeerStateMachine creates a new instance of PeerStateMachine with initial state PeerStatusAlive.
func NewPeerStateMachine() *PeerStateMachine {
	return &PeerStateMachine{
		state:               PeerStatusAlive,
		lastSeen:            time.Time{},
		lastSeenInitialized: false,
		resurrectingChecks:  0,
	}
}

// Get returns the current status of the peer.
func (sm *PeerStateMachine) Get() PeerStatus {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.state
}

// Set updates the status of the peer to the new value.
func (sm *PeerStateMachine) Set(newState PeerStatus) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.state = newState
}

// UpdateLastSeen updates the last seen timestamp to the current time.
func (sm *PeerStateMachine) UpdateLastSeen() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.lastSeen = time.Now()
	sm.lastSeenInitialized = true
}

// GetLastSeen returns the last time the peer was seen.
// If it hasn't been seen yet, it initializes the last seen time to now.
func (sm *PeerStateMachine) GetLastSeen() time.Time {
	sm.mu.Lock()

	if !sm.lastSeenInitialized {
		sm.lastSeen = time.Now()
		sm.lastSeenInitialized = true
	}

	defer sm.mu.Unlock()
	return sm.lastSeen
}

// IncrementResurrectingChecks increments the counter for resurrection checks and returns the new value.
func (sm *PeerStateMachine) IncrementResurrectingChecks() int {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.resurrectingChecks++
	return sm.resurrectingChecks
}

// ResetResurrectingChecks resets the resurrection check counter to zero.
func (sm *PeerStateMachine) ResetResurrectingChecks() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.resurrectingChecks = 0
}
