package watch_mesh

import (
	"common/logger"
	"common/respawner"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/op/go-logging"
)

const (
	UDP_SOCKET_BUFFER_SIZE        = 1024
	OOB_UDP_SOCKET_BUFFER_SIZE    = 128
	SINGLE_ITEM_BUFFER_LEN        = 1
	HEARTBEAT_ACK_ITEM_BUFFER_LEN = 6

	MAX_LEADERLESS_BEATS_BEFORE_STARTING_ELECTIONS = 5
	MAX_JITTER_MILLISECONDS                        = 300
)

type NodeId string

// Peer represents a peer node in the mesh
type Peer struct {
	Address *net.UDPAddr
	State   *PeerStateMachine
}

// Node represents a node in the distributed system
type WatchMesh struct {
	config                          WatchMeshConfig
	conn                            *net.UDPConn
	peers                           map[NodeId]*Peer
	myState                         *MyStateMachine
	leaderID                        NodeId
	discoverResultChan              chan NodeId
	leaderDiscoveryHeartbeatAckChan chan NodeId
	electionCoordinatorAckChan      chan NodeId
	electionOkChan                  chan struct{}
	leaderCancelCh                  chan struct{}
	leaderDiscoveryFinished         bool
	mutex                           sync.Mutex
	log                             *logging.Logger
	respawner                       *respawner.Respawner
	randGenerator                   *rand.Rand
}

// NewNode creates a new distributed node
func NewWatchMesh(config WatchMeshConfig) *WatchMesh {
	logger := logger.GetLoggerWithPrefix("[WATCH-MESH]")

	// Calculate seed by adding character values from the node ID
	idValue := 0
	for _, char := range string(config.CurrentNodeID) {
		idValue += int(char)
	}
	finalSeed := int(config.RandomSeedForJitter) + idValue
	randSource := rand.NewSource(int64(finalSeed))
	randGenerator := rand.New(randSource)

	logger.Infof("Random seed for jitter initialized: %d", finalSeed)

	return &WatchMesh{
		config:                          config,
		conn:                            nil,
		peers:                           make(map[NodeId]*Peer),
		myState:                         NewMyStateMachine(),
		leaderID:                        "",
		discoverResultChan:              make(chan NodeId, SINGLE_ITEM_BUFFER_LEN),
		leaderDiscoveryHeartbeatAckChan: make(chan NodeId, HEARTBEAT_ACK_ITEM_BUFFER_LEN),
		electionCoordinatorAckChan:      make(chan NodeId, HEARTBEAT_ACK_ITEM_BUFFER_LEN),
		electionOkChan:                  make(chan struct{}, HEARTBEAT_ACK_ITEM_BUFFER_LEN),
		leaderDiscoveryFinished:         false,
		mutex:                           sync.Mutex{},
		log:                             logger,
		respawner:                       respawner.NewRespawner(),
		randGenerator:                   randGenerator,
	}
}

func (wm *WatchMesh) Start() {
	go func() {
		wm.setupPeers()
		wm.startUDPListener()

		wm.discoverLeader()

		go wm.runHeartbeat()
	}()
}

// discoverLeader queries all peers for the current leader
func (wm *WatchMesh) discoverLeader() {
	wm.log.Info("Starting LeaderDiscovery")

	// Send LeaderDiscovery messages to all peers
	wm.sendLeaderDiscoveryToPeers()

	// Wait for discovery results with timeout
	select {
	case discoveredLeaderID := <-wm.discoverResultChan:
		wm.handleDiscoveredLeader(discoveredLeaderID)

	case <-time.After(wm.config.HeartbeatInterval):
		wm.handleDiscoveryTimeout()
	}

	// Mark discovery as finished
	wm.mutex.Lock()
	wm.leaderDiscoveryFinished = true
	wm.mutex.Unlock()
}

func (wm *WatchMesh) handleDiscoveredLeader(discoveredLeaderID NodeId) {
	// Validated leader received
	wm.mutex.Lock()
	wm.leaderID = discoveredLeaderID
	wm.myState.Set(StatusAlive)
	wm.mutex.Unlock()
	wm.log.Infof("Leader discovered and validated: %s", discoveredLeaderID)
}

func (wm *WatchMesh) handleDiscoveryTimeout() {
	// Timeout - no validated leader received
	currentState := wm.myState.Get()
	currentLeader := wm.GetLeaderID()

	if wm.shouldStartNewElection(currentState, currentLeader) {
		wm.log.Info("No leader discovered within timeout, starting election")
		wm.startElection(false)
	} else if currentState == StatusElectionStarter && currentLeader == "" {
		wm.log.Info("Timeout but election already in progress")
	}
}

// sendLeaderDiscoveryToPeers sends leader discovery messages to all peers
func (wm *WatchMesh) sendLeaderDiscoveryToPeers() {
	wm.log.Info("Sending LeaderDiscovery messages to all peers")

	// Reset discovery state
	wm.mutex.Lock()
	wm.leaderDiscoveryFinished = false
	wm.mutex.Unlock()

	// Copy peers to avoid holding lock during I/O
	wm.mutex.Lock()
	peerAddrs := make([]*net.UDPAddr, 0, len(wm.peers))
	for _, peer := range wm.peers {
		peerAddrs = append(peerAddrs, peer.Address)
	}
	wm.mutex.Unlock()

	// Send LeaderDiscovery messages to all peers
	for _, addr := range peerAddrs {
		msg := NewLeaderDiscoveryMessage(string(wm.config.CurrentNodeID))
		err := wm.sendMessage(addr, msg)
		if err != nil {
			wm.log.Errorf("Failed to send LeaderDiscovery to %s: %v", addr, err)
		} else {
			wm.log.Infof("Sent LeaderDiscovery to %s", addr)
		}
	}
}

func (wm *WatchMesh) validateFromChan(leaderID NodeId) {
	// Wait for heartbeat acknowledgment with timeout
	timeout := wm.config.HeartbeatInterval
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			wm.mutex.Lock()
			leaderDiscoveryFinished := wm.leaderDiscoveryFinished
			wm.mutex.Unlock()

			if leaderDiscoveryFinished {
				return
			}

			// Timeout - no acknowledgment
			wm.log.Warningf("No acknowledgment from %s within timeout", leaderID)
			return
		case candidateLeader := <-wm.leaderDiscoveryHeartbeatAckChan:
			fullLeaderId := NodeId(wm.composeFullNodeId(leaderID))
			if candidateLeader != fullLeaderId {
				wm.log.Infof("Expected leaderID '%s' but got '%s' for LeaderDiscovery validation", fullLeaderId, candidateLeader)
				continue
			}

			// We could check lastSeen here, but for simplicity assume success
			wm.log.Infof("Leader %s validated successfully", leaderID)
			// Send the validated leader ID through the channel

			wm.mutex.Lock()
			wm.leaderDiscoveryFinished = true
			wm.mutex.Unlock()

			wm.discoverResultChan <- leaderID
			return
		}
	}
}

// validateLeader performs a heartbeat check on a discovered leader
func (wm *WatchMesh) validateLeader(leaderID NodeId, leaderAddr *net.UDPAddr) {
	wm.log.Infof("Validating leader %s", leaderID)

	// Send a specific heartbeat to validate the leader
	msg := NewHeartbeatMessage(string(wm.config.CurrentNodeID))
	if err := wm.sendMessage(leaderAddr, msg); err != nil {
		wm.log.Errorf("Failed to send validation heartbeat: %v", err)
		wm.mutex.Lock()
		wm.leaderDiscoveryFinished = true
		wm.mutex.Unlock()
		return
	}
	wm.validateFromChan(leaderID)
}

// compareNodeIDs compares two NodeIds based on the last character as a number.
// Returns:
//
//	-1 if a < b
//	 0 if a == b or if either ID is empty or the last character is not a digit
//	 1 if a > b
func compareNodeIDs(a, b NodeId) int {
	aStr := string(a)
	bStr := string(b)

	if len(aStr) == 0 || len(bStr) == 0 {
		return 0
	}

	aLast := aStr[len(aStr)-1]
	bLast := bStr[len(bStr)-1]

	aNum, errA := strconv.Atoi(string(aLast))
	bNum, errB := strconv.Atoi(string(bLast))

	if errA != nil || errB != nil {
		return 0
	}

	if aNum < bNum {
		return -1
	} else if aNum > bNum {
		return 1
	}

	return 0
}

// SetupPeers configures the peer addresses from the configuration
func (wm *WatchMesh) setupPeers() {
	var wg sync.WaitGroup

	for _, peerAddress := range wm.config.PeerAddresses {
		wg.Add(1)

		go func(addr string) {
			defer wg.Done()
			wm.resolveAndAddPeer(addr)
		}(peerAddress)

	}
	wg.Wait()
}

// resolveAndAddPeer handles resolving a single peer address with retries
func (wm *WatchMesh) resolveAndAddPeer(peerAddress string) {
	addrWithPort := fmt.Sprintf("%s:%d", peerAddress, wm.config.WatchMeshPort)

	var addrWithPortResolved *net.UDPAddr
	var err error

	for retry := 0; retry < wm.config.AddressResolvingRetries; retry++ {
		addrWithPortResolved, err = net.ResolveUDPAddr("udp", addrWithPort)
		if err == nil {
			break
		}
		if retry < (wm.config.AddressResolvingRetries - 1) {
			time.Sleep(wm.config.AddressResolvingInterval)
		}
		wm.log.Infof(
			"Retrying address resolution for '%v' (%d/%d)",
			addrWithPort, retry+1, wm.config.AddressResolvingRetries,
		)
	}

	if err != nil {
		wm.log.Warningf(
			"Failed to resolve peer address '%v' after %d retries: %v",
			addrWithPort, wm.config.AddressResolvingRetries, err,
		)
		return
	}

	wm.log.Infof("Resolved address for '%v': %v", addrWithPort, addrWithPortResolved)

	wm.mutex.Lock()
	wm.peers[NodeId(peerAddress)] = &Peer{
		Address: addrWithPortResolved,
		State:   NewPeerStateMachine(),
	}
	wm.mutex.Unlock()
}

// listenFromSocket continuously reads UDP packets from the connection
// and passes them to handleMessage for processing. It uses ReadMsgUDP
// to detect when a datagram is too large for the receive buffer.
func (wm *WatchMesh) listenFromSocket(conn *net.UDPConn) {
	// Primary buffer for receiving UDP datagrams.
	buffer := make([]byte, UDP_SOCKET_BUFFER_SIZE)

	// "Out-of-band" data buffer used by ReadMsgUDP for capturing
	// control messages (e.g., TTL, interface info)
	oob := make([]byte, OOB_UDP_SOCKET_BUFFER_SIZE)

	for {
		wm.readAndHandleMessage(conn, buffer, oob)
	}
}

func (wm *WatchMesh) readAndHandleMessage(conn *net.UDPConn, buffer, oob []byte) {
	// ReadMsgUDP reads a single UDP datagram into buffer.
	// It returns the number of bytes read (size),
	// length of the OOB data, flags, remote address, and error.
	size, _, _, remoteAddr, err := conn.ReadMsgUDP(buffer, oob)
	if err != nil {
		wm.log.Warningf("Error reading UDP: %v", err)
		return
	}

	// If the returned size exceeds our buffer length, it means
	// the incoming packet was truncated. The excess bytes are lost.
	if size > len(buffer) {
		wm.log.Warningf(
			"Truncated UDP packet from %s (message too large: %d bytes > buffer %d)",
			remoteAddr, size, len(buffer),
		)
		return
	}

	// Copy only the valid portion of the buffer into msgBytes,
	// since buffer may be reused for future reads.
	msgBytes := make([]byte, size)
	copy(msgBytes, buffer[:size])

	// Delegate to message handler for further processing.
	wm.handleMessage(msgBytes, remoteAddr)
}

// StartUDPListener starts the UDP listener for inter-node communication
func (wm *WatchMesh) startUDPListener() error {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", wm.config.WatchMeshPort))
	if err != nil {
		return fmt.Errorf("failed to resolve UDP address: %v", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on UDP: %v", err)
	}

	wm.conn = conn
	wm.log.Infof("Listening on port %d", wm.config.WatchMeshPort)

	go wm.listenFromSocket(conn)

	return nil
}

func (wm *WatchMesh) sendHeartbeatPings() {
	if wm.AmILeader() {
		wm.sendHeartbeatsToPeers()
	} else {
		wm.sendHeartbeatToLeader()
	}
}

func (wm *WatchMesh) sendHeartbeatsToPeers() {
	peerAddrs := wm.getPeerAddresses()

	if wm.config.ShowHeartbeatLogs {
		wm.log.Infof("[L] Sending heartbeat to peers")
	}

	for _, addr := range peerAddrs {
		msg := NewHeartbeatMessage(string(wm.config.CurrentNodeID))
		if err := wm.sendMessage(addr, msg); err != nil {
			wm.log.Warningf("Failed to send heartbeat to %s: %v", addr, err)
		}
	}
}

func (wm *WatchMesh) sendHeartbeatToLeader() {
	msg := NewHeartbeatMessage(string(wm.config.CurrentNodeID))

	wm.mutex.Lock()
	leaderID := NodeId(wm.composeFullNodeId(wm.leaderID))
	peer, exists := wm.peers[leaderID]
	wm.mutex.Unlock()

	if wm.config.ShowHeartbeatLogs {
		wm.log.Infof("    Sending heartbeat to leader %s", leaderID)
	}

	if exists {
		if err := wm.sendMessage(peer.Address, msg); err != nil {
			wm.log.Warningf("Failed to send heartbeat to %s: %v", peer.Address, err)
		}
	} else {
		wm.log.Warningf("Unknown leaderId: %v", leaderID)
	}
}

func (wm *WatchMesh) getPeerAddresses() []*net.UDPAddr {
	wm.mutex.Lock()
	defer wm.mutex.Unlock()

	peerAddrs := make([]*net.UDPAddr, 0, len(wm.peers))

	for _, peer := range wm.peers {
		peerAddrs = append(peerAddrs, peer.Address)
	}

	return peerAddrs
}

func (wm *WatchMesh) checkLiveness() {
	peerIdsToResurrect := []NodeId{}
	shouldStartElections := false

	if wm.AmILeader() {
		peerIdsToResurrect = wm.checkPeersLiveness()
	} else {
		shouldStartElections = wm.checkLeaderLiveness()
	}

	wm.handleLivenessResults(peerIdsToResurrect, shouldStartElections)
}

func (wm *WatchMesh) checkPeersLiveness() (peerIdsToResurrect []NodeId) {
	peerIdsToResurrect = []NodeId{}

	wm.mutex.Lock()
	// Create a copy of the map to avoid race conditions during iteration
	// and to avoid holding the lock while calling methods that might re-acquire it (deadlock prevention)
	peers := make(map[NodeId]*Peer, len(wm.peers))
	for k, v := range wm.peers {
		peers[k] = v
	}
	wm.mutex.Unlock()

	now := time.Now()

	for id, peer := range peers {
		resurrect, _ := wm.checkPeer(id, peer, now)
		if resurrect {
			peerIdsToResurrect = append(peerIdsToResurrect, id)
		}
	}
	return peerIdsToResurrect
}

func (wm *WatchMesh) checkLeaderLiveness() (shouldStartElections bool) {
	wm.mutex.Lock()
	leaderID := NodeId(wm.composeFullNodeId(wm.leaderID))
	peer, exists := wm.peers[leaderID]
	wm.mutex.Unlock()

	if !exists {
		// Leader doesn't exist in peers map, start elections
		return true
	}

	now := time.Now()
	_, startElection := wm.checkPeer(leaderID, peer, now)

	return startElection
}

func (wm *WatchMesh) checkPeer(id NodeId, peer *Peer, now time.Time) (shouldResurrect bool, shouldStartElection bool) {
	timeSinceLastSeen := now.Sub(peer.State.GetLastSeen())
	currentState := peer.State.Get()

	if timeSinceLastSeen > wm.config.HeartbeatTimeout {
		return wm.handlePeerTimeout(id, peer, currentState, timeSinceLastSeen)
	}

	// timeSinceLastSeen was updated by a HeartbeatAck so set Alive state
	wm.handleAnsweringPeer(id, peer, currentState)
	return false, false
}

func (wm *WatchMesh) handlePeerTimeout(id NodeId, peer *Peer, currentState PeerStatus, timeSinceLastSeen time.Duration) (shouldResurrect bool, shouldStartElection bool) {
	wm.log.Warningf("Heartbeat check: Node %s is down (last seen: %.0f seconds ago)", id, timeSinceLastSeen.Seconds())

	shouldResurrect = false
	shouldStartElection = false

	switch currentState {
	case PeerStatusAlive:
		// Peer was alive and now is found to be dead. Immediate resurrection will be attempted
		shouldResurrect, shouldStartElection = wm.setupStateForPeerResurrection(id, peer)

	case PeerStatusResurrecting:
		// Peer was being resurrected, handle case when exceeded resurrection checks
		shouldStartElection = wm.handleResurrectingPeerTimeout(id, peer)

	case PeerStatusDead:
		// Peer was dead so the peer did not resurrect, trying again
		shouldResurrect, shouldStartElection = wm.setupStateForPeerResurrection(id, peer)
	}

	return shouldResurrect, shouldStartElection
}

func (wm *WatchMesh) setupStateForPeerResurrection(id NodeId, peer *Peer) (shouldResurrect bool, shouldStartElection bool) {
	shouldResurrect = false
	shouldStartElection = false
	leaderId := NodeId(wm.composeFullNodeId(wm.GetLeaderID()))

	if wm.AmILeader() {
		shouldResurrect = true
	}

	if id == leaderId {
		shouldStartElection = true
		peer.State.Set(PeerStatusDead)
		wm.log.Warningf("Leader %s is down", id)
		return shouldResurrect, shouldStartElection
	}

	peer.State.Set(PeerStatusResurrecting)
	peer.State.ResetResurrectingChecks()

	return shouldResurrect, shouldStartElection
}

func (wm *WatchMesh) handleResurrectingPeerTimeout(id NodeId, peer *Peer) (shouldStartElection bool) {
	checks := peer.State.IncrementResurrectingChecks()
	shouldStartElection = false

	maxResurrectionAttempts := wm.config.MaxResurrectionAttempts
	if checks >= maxResurrectionAttempts {
		peer.State.Set(PeerStatusDead)
		wm.log.Warningf("Node %s failed to resurrect after %d checks", id, maxResurrectionAttempts)

		if id == wm.GetLeaderID() {
			shouldStartElection = true
		}
	} else {
		wm.log.Infof("Node %s is in Resurrecting state (check %d/%d)", id, checks, maxResurrectionAttempts)
	}

	return shouldStartElection
}

func (wm *WatchMesh) handleAnsweringPeer(id NodeId, peer *Peer, currentState PeerStatus) {
	if currentState == PeerStatusAlive {
		return
	}

	wm.log.Infof("Node %s recovered, setting to Alive", id)
	peer.State.Set(PeerStatusAlive)
	peer.State.ResetResurrectingChecks()
}

func (wm *WatchMesh) handleLivenessResults(peerIdsToResurrect []NodeId, shouldStartElections bool) {
	if len(peerIdsToResurrect) > 0 {
		for _, peerId := range peerIdsToResurrect {
			wm.resurrectPeer(peerId)
		}
	} else if shouldStartElections {
		wm.startElection(false)
	}
}

func (wm *WatchMesh) runHeartbeat() {
	wm.log.Info("Starting Heartbeat subsystem")
	ticker := time.NewTicker(wm.config.HeartbeatInterval)
	defer ticker.Stop()

	noLeaderCounter := 0

	for range ticker.C {
		hasLeader := wm.GetLeaderID() != ""

		if !hasLeader {
			noLeaderCounter = wm.handleLeaderlessState(noLeaderCounter)
			// Don't send heartbeats until we have a known leader or we're leader
			continue
		}

		noLeaderCounter = 0

		wm.sendHeartbeatPings()
		wm.checkLiveness()
	}
}

func (wm *WatchMesh) handleLeaderlessState(noLeaderCounter int) int {
	if wm.myState.Get() != StatusElectionStarter {

		noLeaderCounter++

		if noLeaderCounter >= MAX_LEADERLESS_BEATS_BEFORE_STARTING_ELECTIONS {
			wm.log.Info("No leader and no election for 5 heartbeats, starting election")
			wm.startElection(false)
			noLeaderCounter = 0
		}

	} else {
		noLeaderCounter = 0
	}

	return noLeaderCounter
}

// AmILeader returns whether this node is the current leader
func (wm *WatchMesh) AmILeader() bool {
	wm.mutex.Lock()
	defer wm.mutex.Unlock()

	return wm.myState.Get() == StatusLeader || wm.leaderID == wm.config.CurrentNodeID
}

// GetLeaderID returns the current leader's ID
func (wm *WatchMesh) GetLeaderID() NodeId {
	wm.mutex.Lock()
	defer wm.mutex.Unlock()

	return wm.leaderID
}

// handleMessage processes incoming UDP messages
func (wm *WatchMesh) handleMessage(msgBytes []byte, addr *net.UDPAddr) {
	msg, err := WatchMeshMessageFromBytes(msgBytes)
	if err != nil {
		wm.log.Warningf("Failed to deserialize message: %v", err)
		return
	}

	// Use composeFullNodeId to ensure consistent key usage
	fullNodeId := NodeId(wm.composeFullNodeId(NodeId(msg.SenderID)))
	wm.updatePeerAddress(fullNodeId, addr)

	switch msg.Type {
	case Heartbeat:
		wm.handleHeartbeatMessage(addr)

	case HeartbeatAck:
		wm.handleHeartbeatAckMessage(msg)

	case Election:
		wm.handleElection(msg.SenderID)

	case Coordinator:
		wm.handleCoordinator(msg.SenderID, addr)

	case CoordinatorAck:
		wm.handleCoordinatorAck(msg)

	case ElectionOk:
		wm.handleElectionOkMessage(msg)

	case LeaderDiscovery:
		wm.handleLeaderDiscoveryMessage(msg, addr)

	case LeaderResponse:
		wm.handleLeaderResponseMessage(msg)
	}
}

// sendMessage sends a UDP message to the specified address,
// retrying until all bytes are written or an error occurs.
func (wm *WatchMesh) sendMessage(addr *net.UDPAddr, msg *WatchMeshMessage) error {
	msgBytes, err := msg.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize message: %w", err)
	}
	totalWritten := 0

	for totalWritten < len(msgBytes) {
		wm.conn.SetWriteDeadline(time.Now().Add(wm.config.HeartbeatInterval))
		n, err := wm.conn.WriteToUDP(
			msgBytes[totalWritten:],
			addr,
		)
		wm.conn.SetWriteDeadline(time.Time{})

		if err != nil {
			return fmt.Errorf("failed to send message: %w", err)
		}

		if n == 0 {
			return fmt.Errorf("no bytes written to UDP connection")
		}

		totalWritten += n
	}

	if totalWritten != len(msgBytes) {
		return fmt.Errorf("short write: wrote %d bytes, expected %d",
			totalWritten, len(msgBytes))
	}

	return nil
}

// shouldStartNewElection determines whether a new election should be started
// based on the current node state and leader information
func (wm *WatchMesh) shouldStartNewElection(currentState CurrentNodeStatus, currentLeader NodeId) bool {
	return currentState != StatusElectionStarter &&
		currentState != StatusLeader &&
		currentLeader == ""
}

// startElection implements the Bully Algorithm for leader election
func (wm *WatchMesh) startElection(skipJitter bool) {
	if !wm.canElectionStart() {
		return
	}
	wm.log.Info("Starting election")

	if !skipJitter {
		wm.waitJitter()
	}

	higherPeers := wm.findHigherPeers()

	if len(higherPeers) == 0 {
		wm.log.Info("No higher ID peers found, becoming leader")
		wm.becomeLeader()
		return
	}

	wm.sendElectionToHigherPeers(higherPeers)
	wm.waitForElectionResult()
}

func (wm *WatchMesh) waitJitter() {
	// Random sleep (0–300ms) to desynchronize simultaneous elections
	delay := time.Duration(wm.randGenerator.Intn(MAX_JITTER_MILLISECONDS)) * time.Millisecond
	wm.log.Infof("Throttling election start by %v to reduce contention", delay)
	time.Sleep(delay)
}

func (wm *WatchMesh) canElectionStart() bool {
	if wm.AmILeader() {
		wm.log.Info("Already leader, ignoring election start request")
		return false
	}

	if wm.myState.Get() == StatusElectionStarter {
		wm.log.Info("Election already in progress, ignoring election start request")
		return false
	}

	wm.myState.Set(StatusElectionStarter)
	return true
}

func (wm *WatchMesh) findHigherPeers() []NodeId {
	higherPeers := []NodeId{}
	wm.mutex.Lock()
	defer wm.mutex.Unlock()

	for id := range wm.peers {
		if compareNodeIDs(id, wm.config.CurrentNodeID) > 0 {
			higherPeers = append(higherPeers, id)
		}
	}

	return higherPeers
}

func (wm *WatchMesh) sendElectionToHigherPeers(higherPeers []NodeId) {
	wm.log.Infof("Sending election messages to higher ID peers: %v", higherPeers)

	for _, id := range higherPeers {
		wm.mutex.Lock()
		peer, ok := wm.peers[id]
		wm.mutex.Unlock()

		if !ok {
			wm.log.Warningf("Could not find peer with id %v", id)
			continue
		}

		msg := NewElectionMessage(string(wm.config.CurrentNodeID))
		err := wm.sendMessage(peer.Address, msg)
		if err != nil {
			wm.log.Warningf("Failed to send ElectionMessage: %v", err)
		} else {
			wm.log.Infof("Sent election message to node %s", id)
		}
	}
}

func (wm *WatchMesh) waitForElectionResult() {
	wm.log.Infof("Waiting up to %v for coordinator message", wm.config.HeartbeatInterval)
	go func(timeout time.Duration) {
		timer := time.NewTimer(timeout)
		defer timer.Stop()

		receivedOk := false

		select {
		case <-wm.electionOkChan:
			receivedOk = true
			wm.log.Info("Received ElectionOk")
			<-timer.C
		case <-timer.C:
			// Timeout
		}

		currentState := wm.myState.Get()
		if currentState != StatusElectionStarter {
			return
		}

		if !receivedOk {
			wm.log.Info("No ElectionOk and no coordinator within timeout; becoming leader")
			wm.becomeLeader()
		} else {
			wm.log.Info("Received ElectionOk but no coordinator within timeout; restarting election")
			wm.myState.Set(StatusAlive)
			wm.startElection(false)
		}
	}(wm.config.HeartbeatInterval)
}

// handleElection handles incoming election messages
func (wm *WatchMesh) handleElection(senderID string) {
	wm.log.Infof("Received election message from node %s", senderID)
	senderNodeID := NodeId(senderID)

	if compareNodeIDs(senderNodeID, wm.config.CurrentNodeID) < 0 {
		wm.handleLowerIDElection(senderID)
	} else {
		wm.log.Infof("Ignoring election message from higher ID node %s", senderID)
	}
}

func (wm *WatchMesh) handleLowerIDElection(senderID string) {
	wm.log.Infof("Responding to lower ID node %s with ElectionOk", senderID)

	senderNodeID := NodeId(senderID)
	wm.mutex.Lock()
	fullSenderNodeID := NodeId(wm.composeFullNodeId(senderNodeID))
	peer, ok := wm.peers[fullSenderNodeID]
	wm.mutex.Unlock()

	if ok {
		wm.sendElectionOk(peer.Address)
	}

	// Check if we should start election
	currentState := wm.myState.Get()

	shouldStartElection := currentState != StatusLeader && currentState != StatusElectionStarter && currentState != StatusCoordinatorCandidate

	if shouldStartElection {
		wm.log.Infof("Starting own election as response to lower ID node %s", senderID)
		wm.startElection(true)
	} else if currentState == StatusElectionStarter {
		wm.log.Infof("Election already in progress, not starting another in response to %s", senderID)
	} else {
		// Leader or CoordinatorCandidate
		wm.log.Infof("Already Leader/Candidate, not starting another in response to %s", senderID)
		if ok {
			if ok {
				wm.assertDominance(senderID, peer.Address)
			}
		}
	}
}

func (wm *WatchMesh) sendElectionOk(addr *net.UDPAddr) {
	msg := NewElectionOkMessage(string(wm.config.CurrentNodeID))
	if err := wm.sendMessage(addr, msg); err != nil {
		wm.log.Warningf("Failed to send ElectionOk: %v", err)
	}
}

func (wm *WatchMesh) assertDominance(targetID string, addr *net.UDPAddr) {
	wm.log.Infof("Asserting dominance to %s with Coordinator message", targetID)
	msg := NewCoordinatorMessage(string(wm.config.CurrentNodeID))
	if err := wm.sendMessage(addr, msg); err != nil {
		wm.log.Warningf("Failed to send asserting Coordinator to %s: %v", addr, err)
	}
}

func (wm *WatchMesh) handleCoordinator(senderID string, addr *net.UDPAddr) {
	wm.log.Infof("Received coordinator message from node %s", senderID)
	senderNodeID := NodeId(senderID)

	wm.mutex.Lock()
	currentNodeID := wm.config.CurrentNodeID
	wm.mutex.Unlock()

	compare := compareNodeIDs(senderNodeID, currentNodeID)

	if compare < 0 {
		wm.handleLowerIDCoordinator(senderNodeID, currentNodeID, addr)
		return
	}

	wm.acceptNewLeader(senderNodeID, senderID, addr)
}

func (wm *WatchMesh) handleLowerIDCoordinator(senderNodeID NodeId, currentNodeID NodeId, addr *net.UDPAddr) {
	// Sender has LOWER ID -> reject its leadership, assert dominance
	currentState := wm.myState.Get()

	if currentState == StatusLeader || currentState == StatusCoordinatorCandidate {
		wm.log.Infof("Received Coordinator from lower ID %s, but already Leader/Candidate. Sending Coordinator to assert dominance.", senderNodeID)
		wm.assertDominance(string(senderNodeID), addr)
		return
	}

	wm.log.Warningf(
		"Received Coordinator from LOWER-ID node %s (self=%s). Refusing ACK and asserting leadership",
		senderNodeID, currentNodeID,
	)

	// Force leader ID reset to ensure becomeLeader doesn't abort if it thinks there's an alive leader
	wm.mutex.Lock()
	wm.leaderID = ""
	wm.mutex.Unlock()

	go wm.becomeLeader()
}

func (wm *WatchMesh) acceptNewLeader(senderNodeID NodeId, senderID string, addr *net.UDPAddr) {
	// Sender has equal or higher ID -> accept it as the new leader
	wm.mutex.Lock()
	if wm.leaderCancelCh != nil {
		close(wm.leaderCancelCh)
		wm.leaderCancelCh = nil
	}
	wm.leaderID = senderNodeID
	wm.mutex.Unlock()

	wm.myState.Set(StatusAlive) // Follower

	wm.log.Infof("New leader elected: node %s", senderID)

	ackMsg := NewCoordinatorAckMessage(string(wm.config.CurrentNodeID))
	if err := wm.sendMessage(addr, ackMsg); err != nil {
		wm.log.Warningf("Failed to send CoordinatorAck to %s: %v", addr, err)
	} else {
		wm.log.Infof("Sent CoordinatorAck to new leader %s (%v)", senderID, addr)
	}
}

// becomeLeader transitions the node to leader state
func (wm *WatchMesh) becomeLeader() {
	wm.log.Info("Becoming leader")

	wm.mutex.Lock()
	currentLeader := wm.leaderID
	leaderAlive := false
	if currentLeader != "" {
		if peer, ok := wm.peers[currentLeader]; ok {
			leaderAlive = peer.State.Get() == PeerStatusAlive
		}
	}
	wm.mutex.Unlock()

	if leaderAlive {
		wm.log.Warningf("Aborting becomeLeader: Current leader %s is still Alive", currentLeader)
		return
	}

	peerList, cancelCh := wm.resetLeaderStateAndGetPeers()
	wm.myState.Set(StatusCoordinatorCandidate)

	pending := make(map[NodeId]bool)
	for id := range peerList {
		pending[id] = true
	}

	wm.broadcastCoordinatorMessages(peerList, &pending, cancelCh)
	wm.waitForCoordinatorAcks(&pending, cancelCh)
}

func (wm *WatchMesh) resetLeaderStateAndGetPeers() (map[NodeId]*Peer, chan struct{}) {
	wm.mutex.Lock()
	defer wm.mutex.Unlock()

	if wm.leaderCancelCh != nil {
		close(wm.leaderCancelCh)
	}

	wm.leaderCancelCh = make(chan struct{})
	wm.leaderID = wm.config.CurrentNodeID

	peerList := make(map[NodeId]*Peer, len(wm.peers))
	for id, peer := range wm.peers {
		peerList[id] = peer
	}
	return peerList, wm.leaderCancelCh
}

func (wm *WatchMesh) broadcastCoordinatorMessages(peerList map[NodeId]*Peer, pending *map[NodeId]bool, cancelCh chan struct{}) {
	wm.log.Infof("Broadcasting Coordinator messages to %d peers with ACK", len(peerList))
	for id, peer := range peerList {
		go wm.sendCoordinatorWithRetries(id, peer.Address, pending, cancelCh)
	}
}

func (wm *WatchMesh) sendCoordinatorWithRetries(peerID NodeId, addr *net.UDPAddr, pending *map[NodeId]bool, cancelCh chan struct{}) {
	maxRetries := wm.config.AddressResolvingRetries
	for attempt := 1; attempt <= maxRetries; attempt++ {
		select {
		case <-cancelCh:
			wm.log.Infof("Leader cancel signal received. Stopping Coordinator retries for %s", peerID)
			return
		default:
		}

		msg := NewCoordinatorMessage(string(wm.config.CurrentNodeID))
		if err := wm.sendMessage(addr, msg); err != nil {
			wm.log.Warningf("Failed to send Coordinator to %s (attempt %d): %v", peerID, attempt, err)
		} else {
			wm.log.Infof("Sent Coordinator message to %s (attempt %d)", peerID, attempt)
		}

		time.Sleep(wm.config.HeartbeatTimeout)

		wm.mutex.Lock()
		exists, stillPending := (*pending)[peerID]
		wm.mutex.Unlock()

		if !exists || !stillPending {
			return
		}
	}
	wm.log.Warningf("No CoordinatorAck from %s after retries", peerID)
}

func (wm *WatchMesh) waitForCoordinatorAcks(pending *map[NodeId]bool, cancelCh chan struct{}) {
	go func() {
		timeout := time.NewTimer(wm.config.HeartbeatTimeout * 3)
		defer timeout.Stop()

		for {
			if wm.myState.Get() != StatusCoordinatorCandidate {
				wm.log.Warning("Lost leadership while waiting for Coordinator ACKs. Aborting pending election loop")
				return
			}

			select {
			case <-cancelCh:
				wm.log.Info("Leader cancel signal. Exiting Coordinator ACK watcher")
				return

			case ackID := <-wm.electionCoordinatorAckChan:
				isElectionFinished := wm.processCoordinatorAck(ackID, pending)
				if isElectionFinished {
					return
				}

			case <-timeout.C:
				wm.handleCoordinatorAckTimeout(pending)
				return
			}
		}
	}()
}

func (wm *WatchMesh) processCoordinatorAck(ackID NodeId, pending *map[NodeId]bool) (isElectionFinished bool) {
	isElectionFinished = false
	wm.mutex.Lock()
	defer wm.mutex.Unlock()

	exists, stillPending := (*pending)[ackID]
	if exists && stillPending {
		delete((*pending), ackID)
		wm.log.Infof("Got CoordinatorAck from %s (%d peers left)", ackID, len(*pending))
	}

	if len((*pending)) == 0 {
		wm.log.Infof("All coordinator ACKs received, stopping timeout watcher.")
		wm.myState.Set(StatusLeader)
		isElectionFinished = true
	}

	return isElectionFinished
}

func (wm *WatchMesh) handleCoordinatorAckTimeout(pending *map[NodeId]bool) {
	wm.mutex.Lock()
	remaining := len(*pending)
	wm.mutex.Unlock()

	if remaining == 0 {
		wm.log.Infof("Timeout expired but all ACKs already received.")
		return
	}

	wm.log.Warningf("Coordinator ACK wait timeout. Still missing %d peers", remaining)
}

func (wm *WatchMesh) handleCoordinatorAck(msg *WatchMeshMessage) {
	wm.log.Infof("Received CoordinatorAck from %s", msg.SenderID)
	nodeId := wm.composeFullNodeId(NodeId(msg.SenderID))
	select {
	case wm.electionCoordinatorAckChan <- NodeId(nodeId):
	default:
		// channel full, drop duplicate ack
		wm.log.Infof("CoordinatorAck channel full or duplicate for %s", msg.SenderID)
	}
}

func (wm *WatchMesh) resurrectPeer(peerId NodeId) {
	if !wm.AmILeader() {
		wm.log.Warning("Cannot resurrect because this node is not leader")
		return
	}

	if string(peerId) == "" {
		wm.log.Warning("Cannot resurrect because node ID is an empty string")
		return
	}

	wm.log.Infof("Resurrecting peer with type '%s'", wm.config.NodeType)
	wm.respawner.Respawn(string(peerId))
	wm.log.Infof("Resurrect command issued for peer with ID '%s'", peerId)
}

func (wm *WatchMesh) handleHeartbeatMessage(addr *net.UDPAddr) {
	// Respond to heartbeat - no lock needed for sendMessage
	ackMsg := NewHeartbeatAckMessage(string(wm.config.CurrentNodeID))

	err := wm.sendMessage(addr, ackMsg)
	if err != nil {
		wm.log.Warningf("Failed to send HeartbeatAck: %v", err)
	}
}

func (wm *WatchMesh) handleHeartbeatAckMessage(msg *WatchMeshMessage) {
	// Update last seen time - do this quickly and unlock
	nodeId := NodeId(wm.composeFullNodeId(NodeId(msg.SenderID)))

	wm.mutex.Lock()
	peer, ok := wm.peers[nodeId]
	if !ok {
		wm.mutex.Unlock()
		wm.log.Warningf("HeartbeatAck from from unknown peerId %s", nodeId)
		return
	}

	peer.State.UpdateLastSeen()
	shouldNotify := !wm.leaderDiscoveryFinished
	wm.mutex.Unlock()

	// Send to channel without holding lock
	if shouldNotify {
		select {
		case wm.leaderDiscoveryHeartbeatAckChan <- nodeId:
		default:
			wm.log.Debugf("LeaderDiscoveryHeartbeatAckChan full, dropping ack from %s", nodeId)
		}
	}

	if wm.config.ShowHeartbeatLogs {
		leaderPrefix := "   "
		if string(wm.GetLeaderID()) == msg.SenderID {
			leaderPrefix = "[L]"
		}

		wm.log.Infof("HeartbeatAck from %s '%s'", leaderPrefix, msg.SenderID)
	}
}

func (wm *WatchMesh) handleElectionOkMessage(msg *WatchMeshMessage) {
	wm.log.Infof("Received ElectionOk from node %s", msg.SenderID)
	select {
	case wm.electionOkChan <- struct{}{}:
	default:
		wm.log.Debugf("ElectionOkChan full, dropping message from %s", msg.SenderID)
	}
}

func (wm *WatchMesh) handleLeaderDiscoveryMessage(msg *WatchMeshMessage, addr *net.UDPAddr) {
	// Read state without holding lock
	leaderID := wm.GetLeaderID()

	if leaderID == "" {
		wm.log.Infof("Received LeaderDiscovery request from %s but leader is unknown", msg.SenderID)
		return
	}

	nodeCondition := "follower"
	if wm.AmILeader() {
		nodeCondition = "leader"
	}

	wm.log.Infof("Responding to LeaderDiscovery request from %s as %s, the leader is '%s'", msg.SenderID, nodeCondition, leaderID)
	newMsg := NewLeaderResponseMessage(string(wm.config.CurrentNodeID), string(leaderID))
	err := wm.sendMessage(addr, newMsg)
	if err != nil {
		wm.log.Warningf("Failed to send LeaderResponse: %v", err)
	}
}

func (wm *WatchMesh) handleLeaderResponseMessage(msg *WatchMeshMessage) {
	leaderID := NodeId(msg.Payload)
	if leaderID == "" {
		return
	}

	wm.log.Infof("Received LeaderDiscovery response from %s claiming leader is %s, validating...", msg.SenderID, leaderID)
	// validateLeader is called in a goroutine to avoid blocking message handling
	fullNodeId := NodeId(wm.composeFullNodeId(leaderID))
	wm.mutex.Lock()
	discoveryInProgress := !wm.leaderDiscoveryFinished
	leaderInfo, ok := wm.peers[fullNodeId]
	wm.mutex.Unlock()

	if !ok {
		wm.log.Warningf("Unknown leader %s during LeaderDiscovery", fullNodeId)
		return
	}

	if discoveryInProgress {
		go wm.validateLeader(leaderID, leaderInfo.Address)
	}
}

func (wm *WatchMesh) composeFullNodeId(id NodeId) string {
	return fmt.Sprintf("%s%s", wm.config.NodeType, id)
}

// updatePeerAddress updates the address of a peer if it exists, or adds a new entry if it doesn't
func (wm *WatchMesh) updatePeerAddress(id NodeId, addr *net.UDPAddr) {
	wm.mutex.Lock()
	defer wm.mutex.Unlock()

	peer, exists := wm.peers[id]
	if exists {
		peer.Address = addr
	} else {
		// Peer doesn't exist, add new entry
		wm.peers[id] = &Peer{
			Address: addr,
			State:   NewPeerStateMachine(),
		}
	}
}
