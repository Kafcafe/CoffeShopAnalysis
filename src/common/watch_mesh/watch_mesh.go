package watch_mesh

import (
	"common/logger"
	"fmt"
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
)

type NodeId string

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

// Config holds the configuration for a distributed node
type WatchMeshConfig struct {
	CurrentNodeID            NodeId
	WatchMeshPort            int
	PeerAddresses            []string
	NodeType                 string
	HeartbeatInterval        time.Duration
	HeartbeatTimeout         time.Duration
	AddressResolvingRetries  int
	AddressResolvingInterval time.Duration
}

// NewWatchMeshConfig creates a new WatchMeshConfig with the provided parameters
func NewWatchMeshConfig(currentNodeID string, watchMeshPort int, peerAddresses []string, heartbeatInterval, heartbeatTimeout time.Duration, addressResolvingRetries int, addressResolvingInterval time.Duration) WatchMeshConfig {
	return WatchMeshConfig{
		CurrentNodeID:            NodeId(currentNodeID),
		WatchMeshPort:            watchMeshPort,
		PeerAddresses:            peerAddresses,
		HeartbeatInterval:        heartbeatInterval,
		HeartbeatTimeout:         heartbeatTimeout,
		AddressResolvingRetries:  addressResolvingRetries,
		AddressResolvingInterval: addressResolvingInterval,
	}
}

// Node represents a node in the distributed system
type WatchMesh struct {
	config                          WatchMeshConfig
	conn                            *net.UDPConn
	peers                           map[NodeId]*net.UDPAddr
	isLeader                        bool
	leaderID                        NodeId
	lastSeen                        map[NodeId]time.Time
	electionInProgress              bool
	electionReceivedOK              bool
	discoverResultChan              chan NodeId
	leaderDiscoveryHeartbeatAckChan chan NodeId
	leaderDiscoveryFinished         bool
	mutex                           sync.Mutex
	log                             *logging.Logger
}

// NewNode creates a new distributed node
func NewWatchMesh(config WatchMeshConfig) *WatchMesh {
	logger := logger.GetLoggerWithPrefix("[WATCH-MESH]")

	return &WatchMesh{
		config:                          config,
		conn:                            nil,
		peers:                           make(map[NodeId]*net.UDPAddr),
		isLeader:                        false,
		leaderID:                        "",
		lastSeen:                        make(map[NodeId]time.Time),
		electionInProgress:              false,
		electionReceivedOK:              false,
		discoverResultChan:              make(chan NodeId, SINGLE_ITEM_BUFFER_LEN),
		leaderDiscoveryHeartbeatAckChan: make(chan NodeId, HEARTBEAT_ACK_ITEM_BUFFER_LEN),
		leaderDiscoveryFinished:         false,
		mutex:                           sync.Mutex{},
		log:                             logger,
	}
}

func (wm *WatchMesh) Start() {
	wm.setupPeers()
	wm.startUDPListener()

	wm.discoverLeader()

	go wm.startHeartbeat()
	go wm.startElectionMonitor()
}

// discoverLeader queries all peers for the current leader
func (wm *WatchMesh) discoverLeader() {
	wm.log.Info("Starting LeaderDiscovery")

	select {
	case discoveredLeaderID := <-wm.discoverResultChan:
		// Validated leader received
		wm.mutex.Lock()
		wm.leaderID = discoveredLeaderID
		wm.isLeader = false
		wm.mutex.Unlock()
		wm.log.Infof("Leader discovered and validated: %s", discoveredLeaderID)

	case <-time.After(wm.config.HeartbeatInterval):
		// Timeout - no validated leader received
		wm.mutex.Lock()
		electionInProgress := wm.electionInProgress
		currentLeader := wm.leaderID
		wm.mutex.Unlock()

		if !electionInProgress && currentLeader == "" {
			wm.log.Info("No leader discovered within timeout, starting election")
			// Start election - not holding lock
			wm.startElection()
		} else if electionInProgress && currentLeader == "" {
			wm.log.Info("Timeout but election already in progress")
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
			// Timeout - no acknowledgment
			wm.log.Warningf("No acknowledgment from %s within timeout", leaderID)
			return
		case candidateLeader := <-wm.leaderDiscoveryHeartbeatAckChan:
			if candidateLeader != leaderID {
				continue
			}

			// We could check lastSeen here, but for simplicity assume success
			wm.log.Infof("Leader %s validated successfully", leaderID)
			// Send the validated leader ID through the channel
			wm.discoverResultChan <- leaderID
		}
	}
}

// validateLeader performs a heartbeat check on a discovered leader
func (wm *WatchMesh) validateLeader(leaderID NodeId, responseAddr *net.UDPAddr) {
	wm.log.Infof("Validating leader %s", leaderID)

	// Send a specific heartbeat to validate the leader
	msg := NewHeartbeatMessage(string(wm.config.CurrentNodeID))
	if err := wm.sendMessage(responseAddr, msg); err != nil {
		wm.log.Errorf("Failed to send validation heartbeat: %v", err)
		wm.mutex.Lock()
		wm.leaderDiscoveryFinished = true
		wm.mutex.Unlock()
		return
	}
	wm.validateFromChan(leaderID)
	wm.mutex.Lock()
	wm.leaderDiscoveryFinished = true
	wm.mutex.Unlock()
}

// SetupPeers configures the peer addresses from the configuration
func (wm *WatchMesh) setupPeers() {
	for _, peerAddress := range wm.config.PeerAddresses {
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
			wm.log.Infof("Retrying address resolution for '%v'", addrWithPort)
		}
		if err != nil {
			wm.log.Warningf("Failed to resolve peer address '%v' after 3 retries: %v", addrWithPort, err)
			continue
		}
		wm.log.Infof("Resolved address for '%v': %v", addrWithPort, addrWithPortResolved)
		wm.peers[NodeId(peerAddress)] = addrWithPortResolved
	}
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
		// ReadMsgUDP reads a single UDP datagram into buffer.
		// It returns the number of bytes read (size),
		// length of the OOB data, flags, remote address, and error.
		size, _, _, remoteAddr, err := conn.ReadMsgUDP(buffer, oob)
		if err != nil {
			wm.log.Warningf("Error reading UDP: %v", err)
			continue
		}

		// If the returned size exceeds our buffer length, it means
		// the incoming packet was truncated. The excess bytes are lost.
		if size > len(buffer) {
			wm.log.Warningf(
				"Truncated UDP packet from %s (message too large: %d bytes > buffer %d)",
				remoteAddr, size, len(buffer),
			)
			continue
		}

		// Copy only the valid portion of the buffer into msgBytes,
		// since buffer may be reused for future reads.
		msgBytes := make([]byte, size)
		copy(msgBytes, buffer[:size])

		// Delegate to message handler for further processing.
		wm.handleMessage(msgBytes, remoteAddr)
	}
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

	go func() {
		wm.listenFromSocket(conn)
	}()

	return nil
}

func (wm *WatchMesh) sendHeartbeatPings() {
	amILeaderPrefix := "   "
	wm.mutex.Lock()

	if wm.isLeader {
		amILeaderPrefix = "[L]"
	}

	wm.log.Infof("%s Sending heartbeat to peers", amILeaderPrefix)

	// Copy peers to avoid holding lock during I/O
	peerAddrs := make([]*net.UDPAddr, 0, len(wm.peers))
	for _, addr := range wm.peers {
		peerAddrs = append(peerAddrs, addr)
	}
	wm.mutex.Unlock()

	// Send messages WITHOUT holding the lock
	for _, addr := range peerAddrs {
		msg := NewHeartbeatMessage(string(wm.config.CurrentNodeID))
		wm.sendMessage(addr, msg)
	}
}

func (wm *WatchMesh) checkLiveness() {
	peerIdsToResurrect := []NodeId{}
	shouldStartElections := false

	wm.mutex.Lock()
	now := time.Now()

	for id, last := range wm.lastSeen {
		if now.Sub(last) > wm.config.HeartbeatTimeout {
			wm.log.Warningf("Heartbeat check: Node %s is down (last seen: %.0f seconds ago)", id, now.Sub(last).Seconds())

			if id == wm.leaderID {
				shouldStartElections = true
			} else if wm.isLeader {
				peerIdsToResurrect = append(peerIdsToResurrect, id)
			}
		}
	}
	// Unlock before calling functions that might acquire the mutex
	wm.mutex.Unlock()

	if len(peerIdsToResurrect) > 0 {
		for _, peerId := range peerIdsToResurrect {
			wm.resurrectPeer(peerId)
		}
	}

	if shouldStartElections {
		wm.startElection()
	}
}

func (wm *WatchMesh) startHeartbeat() {
	wm.log.Info("Starting Heartbeat")
	ticker := time.NewTicker(wm.config.HeartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		wm.sendHeartbeatPings()
		wm.checkLiveness()
	}
}

func (wm *WatchMesh) startElectionMonitor() {
	wm.log.Info("Starting election monitoring")
	ticker := time.NewTicker(wm.config.HeartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		wm.mutex.Lock()
		noLeader := wm.leaderID == ""
		inProgress := wm.electionInProgress
		wm.mutex.Unlock()

		// Only start election if conditions are still met (avoid race)
		if noLeader && !inProgress {
			wm.log.Info("ElectionMonitor: No leader known, triggering election")
			wm.startElection()
		}
	}
}

// IsLeader returns whether this node is the current leader
func (wm *WatchMesh) IsLeader() bool {
	wm.mutex.Lock()
	defer wm.mutex.Unlock()
	return wm.isLeader
}

// GetLeaderID returns the current leader's ID
func (wm *WatchMesh) GetLeaderID() string {
	wm.mutex.Lock()
	defer wm.mutex.Unlock()
	return string(wm.leaderID)
}

// handleMessage processes incoming UDP messages
func (wm *WatchMesh) handleMessage(msgBytes []byte, addr *net.UDPAddr) {
	msg, err := FromBytes(msgBytes)
	if err != nil {
		wm.log.Warningf("Failed to deserialize message: %v", err)
		return
	}

	switch msg.Type {
	case Heartbeat:
		// Respond to heartbeat - no lock needed for sendMessage
		ackMsg := NewHeartbeatAckMessage(string(wm.config.CurrentNodeID))
		wm.sendMessage(addr, ackMsg)

	case HeartbeatAck:
		// Update last seen time - do this quickly and unlock
		wm.mutex.Lock()
		nodeId := NodeId(msg.SenderID)
		wm.lastSeen[nodeId] = time.Now()

		// Don't block on channel send - use non-blocking approach
		shouldNotify := !wm.leaderDiscoveryFinished
		wm.mutex.Unlock()

		// Try to send to channel without holding lock
		if shouldNotify {
			select {
			case wm.leaderDiscoveryHeartbeatAckChan <- nodeId:
			default:
				// Channel full, skip notification
			}
		}

		wm.log.Infof("HeartbeatAck from '%s'", msg.SenderID)

	case Election:
		wm.handleElection(msg.SenderID)

	case Coordinator:
		wm.handleCoordinator(msg.SenderID)

	case ElectionOk:
		wm.log.Infof("Received ElectionOk from node %s", msg.SenderID)

	case LeaderDiscovery:
		// Read state without holding lock
		wm.mutex.Lock()
		leaderID := wm.leaderID
		amILeader := wm.isLeader
		wm.mutex.Unlock()

		if leaderID != "" {
			nodeCondition := "follower"
			if amILeader {
				nodeCondition = "leader"
			}

			wm.log.Infof("Responding to LeaderDiscovery request from %s as %s, the leader is '%s'", msg.SenderID, nodeCondition, leaderID)
			msg := NewLeaderResponseMessage(string(wm.config.CurrentNodeID), string(leaderID))
			wm.sendMessage(addr, msg)
		}

	case LeaderResponse:
		leaderID := NodeId(msg.Payload)
		if leaderID != "" {
			wm.log.Infof("Received LeaderDiscovery response from %s claiming leader is %s, validating...", msg.SenderID, leaderID)
			// validateLeader is called in a goroutine to avoid blocking message handling
			// But we need to be careful about leaderDiscoveryFinished flag
			wm.mutex.Lock()
			discoveryInProgress := !wm.leaderDiscoveryFinished
			wm.mutex.Unlock()

			if discoveryInProgress {
				go wm.validateLeader(leaderID, addr)
			}
		}

	case Broadcast:
		wm.log.Infof("Received broadcast from leader %s: %s", msg.SenderID, msg.Payload)
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

// startElection implements the Bully Algorithm for leader election
func (wm *WatchMesh) startElection() {
	// Guard against concurrent elections
	wm.mutex.Lock()
	if wm.electionInProgress {
		wm.log.Warning("Election already in progress, ignoring election start request")
		wm.mutex.Unlock()
		return
	}
	wm.electionInProgress = true
	wm.electionReceivedOK = false
	wm.mutex.Unlock()

	wm.log.Info("Starting election")
	higherPeers := []NodeId{}
	for id := range wm.peers {
		if compareNodeIDs(id, wm.config.CurrentNodeID) > 0 {
			higherPeers = append(higherPeers, id)
		}
	}

	if len(higherPeers) == 0 {
		wm.log.Info("No higher ID peers found, becoming leader")
		// Update election state before calling becomeLeader
		wm.mutex.Lock()
		wm.electionInProgress = false
		wm.mutex.Unlock()
		// becomeLeader will acquire its own lock
		wm.becomeLeader()
		return
	}

	wm.log.Infof("Sending election messages to higher ID peers: %v", higherPeers)
	// Send election messages to higher ID peers
	for _, id := range higherPeers {
		if addr, ok := wm.peers[id]; ok {
			// Sender must be this node's ID
			msg := NewElectionMessage(string(wm.config.CurrentNodeID))
			wm.sendMessage(addr, msg)
			wm.log.Infof("Sent election message to node %s", id)
		}
	}

	wm.log.Infof("Waiting up to %v for coordinator message", wm.config.HeartbeatTimeout)
	// Non-blocking wait for coordinator: use a timer in a goroutine
	go func(timeout time.Duration) {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		<-timer.C

		shouldBecomeLeader := false
		shouldRestartElection := false

		wm.mutex.Lock()
		if wm.electionInProgress && !wm.isLeader {
			if !wm.electionReceivedOK {
				wm.log.Info("No ElectionOk and no coordinator within timeout; becoming leader")
				wm.electionInProgress = false
				shouldBecomeLeader = true
			} else {
				wm.log.Info("Received ElectionOk but no coordinator within timeout; restarting election")
				wm.electionInProgress = false
				shouldRestartElection = true
			}
		}
		wm.mutex.Unlock()

		if shouldBecomeLeader {
			wm.becomeLeader()
		} else if shouldRestartElection {
			// Use a flag to prevent deep recursion
			// This is safe because we set electionInProgress=false above
			wm.startElection()
		}
	}(wm.config.HeartbeatTimeout)
}

// handleElection handles incoming election messages
func (wm *WatchMesh) handleElection(senderID string) {
	wm.log.Infof("Received election message from node %s", senderID)
	senderNodeID := NodeId(senderID)

	if compareNodeIDs(senderNodeID, wm.config.CurrentNodeID) < 0 {
		wm.log.Infof("Responding to lower ID node %s with ElectionOk", senderID)
		// Respond to lower ID node - check if we need to start our own election
		wm.mutex.Lock()
		addr, ok := wm.peers[senderNodeID]
		wm.mutex.Unlock()

		// Send message WITHOUT holding lock
		if ok {
			msg := NewElectionOkMessage(string(wm.config.CurrentNodeID))
			wm.sendMessage(addr, msg)
		}

		// Check if we should start election (don't hold lock during this check to avoid nested locks)
		wm.mutex.Lock()
		stillNotLeader := !wm.isLeader
		stillNotInProgress := !wm.electionInProgress
		wm.mutex.Unlock()

		if stillNotLeader && stillNotInProgress {
			wm.log.Info("Starting own election as response to lower ID node")
			wm.startElection()
		} else {
			wm.log.Info("Election already in progress or already leader, not starting another")
		}
	} else {
		wm.log.Infof("Ignoring election message from higher/equal ID node %s", senderID)
	}
}

// handleCoordinator handles incoming coordinator messages
func (wm *WatchMesh) handleCoordinator(senderID string) {
	wm.log.Infof("Received coordinator message from node %s", senderID)

	wm.mutex.Lock()
	wm.leaderID = NodeId(senderID)
	wm.isLeader = false
	wm.electionInProgress = false
	wm.electionReceivedOK = false
	wm.mutex.Unlock()

	wm.log.Infof("New leader elected: node %s", senderID)
}

// becomeLeader sets this node as the leader
func (wm *WatchMesh) becomeLeader() {
	wm.log.Info("Becoming leader")

	// First, copy peers list and get state
	wm.mutex.Lock()
	wm.isLeader = true
	wm.leaderID = wm.config.CurrentNodeID
	wm.electionInProgress = false
	wm.electionReceivedOK = false

	// Copy peer map to avoid holding lock during I/O
	peerList := make(map[NodeId]*net.UDPAddr, len(wm.peers))
	for id, addr := range wm.peers {
		peerList[id] = addr
	}
	wm.mutex.Unlock()

	var peer_keys []string
	for k := range peerList {
		peer_keys = append(peer_keys, string(k))
	}

	wm.log.Infof("Broadcasting coordinator message to all %d peers", len(peer_keys))

	// Send messages WITHOUT holding the lock
	for id, addr := range peerList {
		msg := NewCoordinatorMessage(string(wm.config.CurrentNodeID))
		wm.sendMessage(addr, msg)
		wm.log.Infof("Sent coordinator message to node %s", id)
	}
}

func (wm *WatchMesh) resurrectPeer(peerId NodeId) {
	wm.mutex.Lock()
	defer wm.mutex.Unlock()

	if !wm.isLeader || string(peerId) == "" {
		wm.log.Warning("Cannot resurrect")
		return
	}

	wm.log.Infof("Trying to resurrect peer with ID '%s'. FEATURE NOT IMPLEMENTED", string(peerId))
}
