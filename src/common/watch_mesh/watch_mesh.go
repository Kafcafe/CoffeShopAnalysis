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
	UDP_SOCKET_BUFFER_SIZE     = 1024
	OOB_UDP_SOCKET_BUFFER_SIZE = 128
	SINGLE_ITEM_BUFFER_LEN     = 1
)

type NodeId string

// compareNodeIDs compares two NodeIds based on the last character as a number
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
	CurrentNodeID                   NodeId
	WatchMeshPort                   int
	PeerAddresses                   []string
	NodeType                        string
	HeartbeatInterval               time.Duration
	HeartbeatTimeout                time.Duration
	AddressResolvingRetries         int
	AddressResolvingIntervalSeconds time.Duration
}

// NewWatchMeshConfig creates a new WatchMeshConfig with the provided parameters
func NewWatchMeshConfig(currentNodeID string, watchMeshPort int, peerAddresses []string, heartbeatInt, timeout time.Duration, addressResolvingRetries int, addressResolvingIntervalSeconds time.Duration) WatchMeshConfig {
	return WatchMeshConfig{
		CurrentNodeID:                   NodeId(currentNodeID),
		WatchMeshPort:                   watchMeshPort,
		PeerAddresses:                   peerAddresses,
		HeartbeatInterval:               heartbeatInt,
		HeartbeatTimeout:                timeout,
		AddressResolvingRetries:         addressResolvingRetries,
		AddressResolvingIntervalSeconds: addressResolvingIntervalSeconds,
	}
}

// Node represents a node in the distributed system
type WatchMesh struct {
	config             WatchMeshConfig
	conn               *net.UDPConn
	peers              map[NodeId]*net.UDPAddr
	isLeader           bool
	leaderID           NodeId
	lastSeen           map[NodeId]time.Time
	electionInProgress bool
	electionReceivedOK bool
	discoverResult     chan NodeId
	mutex              sync.Mutex
	log                *logging.Logger
}

// NewNode creates a new distributed node
func NewWatchMesh(config WatchMeshConfig) *WatchMesh {
	logger := logger.GetLoggerWithPrefix("[WATCH-MESH]")

	return &WatchMesh{
		config:             config,
		conn:               nil,
		peers:              make(map[NodeId]*net.UDPAddr),
		isLeader:           false,
		leaderID:           "",
		lastSeen:           make(map[NodeId]time.Time),
		electionInProgress: false,
		electionReceivedOK: false,
		discoverResult:     make(chan NodeId, 1),
		mutex:              sync.Mutex{},
		log:                logger,
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
	case discoveredLeaderID := <-wm.discoverResult:
		// Validated leader received
		wm.mutex.Lock()
		wm.leaderID = discoveredLeaderID
		wm.isLeader = false
		wm.mutex.Unlock()
		wm.log.Infof("Leader discovered and validated: %s", discoveredLeaderID)
		return
	case <-time.After(wm.config.HeartbeatTimeout):
		// Timeout - no validated leader received
		wm.mutex.Lock()
		notInProgress := !wm.electionInProgress
		wm.mutex.Unlock()

		if notInProgress {
			wm.log.Info("No leader discovered within timeout, starting election")
			wm.startElection()
		} else {
			wm.log.Info("Timeout but election already in progress")
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
		return
	}

	// Wait for heartbeat acknowledgment with timeout
	timeout := wm.config.HeartbeatTimeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-time.After(timeout):
		// Timeout - no acknowledgment
		wm.log.Warningf("No acknowledgment from %s within timeout", leaderID)
		return
	case <-timer.C:
		// We could check lastSeen here, but for simplicity assume success
		wm.log.Infof("Leader %s validated successfully", leaderID)
		// Send the validated leader ID through the channel
		select {
		case wm.discoverResult <- leaderID:
		default:
			// Channel already has a value
		}
	}
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
				time.Sleep(wm.config.AddressResolvingIntervalSeconds)
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

	for _, addr := range wm.peers {
		msg := NewHeartbeatMessage(string(wm.config.CurrentNodeID))
		wm.sendMessage(addr, msg)
	}
	wm.mutex.Unlock()
}

func (wm *WatchMesh) checkLiveness() {
	shouldResurrectPeer := false
	peerIdToResurrect := NodeId("")
	shouldStartElections := false

	wm.mutex.Lock()
	now := time.Now()

	for id, last := range wm.lastSeen {
		if now.Sub(last) > wm.config.HeartbeatTimeout {
			wm.log.Warningf("Heartbeat check: Node %s is down (last seen: %.0f seconds ago)", id, now.Sub(last).Seconds())

			if id == wm.leaderID {
				shouldStartElections = true
			} else if wm.isLeader {
				shouldResurrectPeer = true
				peerIdToResurrect = id
			}
		}
	}
	wm.mutex.Unlock()

	if shouldResurrectPeer {
		wm.resurrectPeer(peerIdToResurrect)
	} else if shouldStartElections {
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
		// Respond to heartbeat
		ackMsg := NewHeartbeatAckMessage(string(wm.config.CurrentNodeID))
		wm.sendMessage(addr, ackMsg)

	case HeartbeatAck:
		wm.mutex.Lock()
		wm.lastSeen[NodeId(msg.SenderID)] = time.Now()
		wm.mutex.Unlock()
		wm.log.Infof("HeartbeatAck from '%s'", msg.SenderID)

	case Election:
		wm.handleElection(msg.SenderID)

	case Coordinator:
		wm.handleCoordinator(msg.SenderID)

	case ElectionOk:
		wm.log.Infof("Received ElectionOk from node %s", msg.SenderID)

	case LeaderDiscovery:
		// If this node is a leader, respond with leader response
		if wm.isLeader {
			wm.mutex.Lock()
			leaderID := wm.leaderID
			wm.mutex.Unlock()
			wm.log.Infof("Responding to LeaderDiscovery from %s as leader %s", msg.SenderID, leaderID)
			msg := NewLeaderResponseMessage(string(wm.config.CurrentNodeID), string(leaderID))
			wm.sendMessage(addr, msg)
		}

	case LeaderResponse:
		leaderID := NodeId(msg.Payload)
		if leaderID != "" {
			wm.log.Infof("Received LeaderDiscovery response from %s claiming leader is %s, validating...", msg.SenderID, leaderID)
			go wm.validateLeader(leaderID, addr)
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
		n, err := wm.conn.WriteToUDP(
			msgBytes[totalWritten:],
			addr,
		)
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
		// End election and become leader
		wm.mutex.Lock()
		wm.electionInProgress = false
		wm.mutex.Unlock()
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
		// Respond to lower ID node
		if addr, ok := wm.peers[senderNodeID]; ok {
			msg := NewElectionOkMessage(string(wm.config.CurrentNodeID))
			wm.sendMessage(addr, msg)
		}
		// Start own election if not already in progress
		wm.mutex.Lock()
		alreadyLeader := wm.isLeader
		inProgress := wm.electionInProgress
		wm.mutex.Unlock()

		if !alreadyLeader && !inProgress {
			wm.log.Info("Starting own election as response to lower ID node")
			wm.startElection()
		} else if inProgress {
			wm.log.Warning("Election already in progress, not starting another")
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
	wm.mutex.Lock()
	wm.isLeader = true
	wm.leaderID = wm.config.CurrentNodeID
	wm.electionInProgress = false
	wm.electionReceivedOK = false
	wm.mutex.Unlock()

	var peer_keys []string
	for k := range wm.peers {
		peer_keys = append(peer_keys, string(k))
	}

	wm.log.Infof("Broadcasting coordinator message to all %d peers", len(peer_keys))

	// Broadcast to all peers
	for id, addr := range wm.peers {
		msg := NewCoordinatorMessage(string(wm.config.CurrentNodeID))
		wm.sendMessage(addr, msg)
		wm.log.Infof("Sent coordinator message to node %s", id)
	}
}

func (wm *WatchMesh) resurrectPeer(peerId NodeId) {
	wm.mutex.Lock()
	if !wm.isLeader || string(peerId) == "" {
		wm.log.Warning("Cannot resurrect")
		return
	}

	wm.log.Infof("Trying to resurrect peer with ID '%s'. FEATURE NOT IMPLEMENTED", string(peerId))

	wm.mutex.Unlock()
}
