package watch_mesh

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	UDP_SOCKET_BUFFER_SIZE = 1024
)

type NodeId string

// Config holds the configuration for a distributed node
type WatchMeshConfig struct {
	CurrentNodeID     NodeId
	WatchMeshPort     int
	PeerIPs           []string
	NodeType          string
	HeartbeatInterval time.Duration
	Timeout           time.Duration
}

// NewWatchMeshConfig creates a new WatchMeshConfig with the provided parameters
func NewWatchMeshConfig(currentNodeID string, watchMeshPort int, peerIPs []string, nodeType string, heartbeatInt, timeout time.Duration) WatchMeshConfig {
	return WatchMeshConfig{
		CurrentNodeID:     NodeId(currentNodeID),
		WatchMeshPort:     watchMeshPort,
		PeerIPs:           peerIPs,
		NodeType:          nodeType,
		HeartbeatInterval: heartbeatInt,
		Timeout:           timeout,
	}
}

// Node represents a node in the distributed system
type WatchMesh struct {
	config   WatchMeshConfig
	conn     *net.UDPConn
	peers    map[NodeId]*net.UDPAddr
	isLeader bool
	leaderID NodeId
	lastSeen map[NodeId]time.Time
	mutex    sync.Mutex
	logger   *log.Logger
}

// NewNode creates a new distributed node
func NewWatchMesh(config WatchMeshConfig, logger *log.Logger) *WatchMesh {
	return &WatchMesh{
		config:   config,
		peers:    make(map[NodeId]*net.UDPAddr),
		lastSeen: make(map[NodeId]time.Time),
		logger:   logger,
	}
}

func (wm *WatchMesh) Start() {
	wm.setupPeers()
	wm.startUDPListener()
	go wm.startHeartbeat()
	go wm.startElectionMonitor()
}

// SetupPeers configures the peer addresses from the configuration
func (wm *WatchMesh) setupPeers() {
	for _, peerIp := range wm.config.PeerIPs {
		addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", peerIp, wm.config.WatchMeshPort))
		if err != nil {
			wm.logger.Printf("Failed to resolve peer address: %v", err)
			continue
		}
		wm.peers[NodeId(peerIp)] = addr
	}
}

func (wm *WatchMesh) listenFromSocket(conn *net.UDPConn) {
	buffer := make([]byte, UDP_SOCKET_BUFFER_SIZE)
	for {
		size, remoteAddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			wm.logger.Printf("Error reading UDP: %v", err)
			continue
		}
		msgBytes := make([]byte, size)
		copy(msgBytes, buffer[:size])
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
	wm.logger.Printf("Listening on port %d", wm.config.WatchMeshPort)

	go func() {
		wm.listenFromSocket(conn)
	}()

	return nil
}

func (wm *WatchMesh) startHeartbeat() {
	ticker := time.NewTicker(wm.config.HeartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		wm.mutex.Lock()
		for _, addr := range wm.peers {
			msg := NewHeartbeatMessage(string(wm.config.CurrentNodeID))
			wm.sendMessage(addr, msg)
		}
		wm.mutex.Unlock()
	}
}

// StartElectionMonitor starts monitoring for failed nodes and triggers elections
func (wm *WatchMesh) startElectionMonitor() {
	ticker := time.NewTicker(wm.config.HeartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		wm.mutex.Lock()
		now := time.Now()
		for id, last := range wm.lastSeen {
			if now.Sub(last) > wm.config.Timeout {
				wm.logger.Printf("Heartbeat check: Node %d is down (last seen: %v)", id, last)
				if id == wm.leaderID {
					wm.startElection()
				}
			} else {
				wm.logger.Printf("Heartbeat check: Node %d is alive (last seen: %v)", id, last)
			}
		}
		wm.mutex.Unlock()
	}
}

// StartInitialElection initiates the first leader election
func (wm *WatchMesh) StartInitialElection() {
	wm.logger.Printf("System startup: Initiating initial leader election")
	wm.startElection()
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
		wm.logger.Printf("Failed to deserialize message: %v", err)
		return
	}

	wm.mutex.Lock()
	wm.lastSeen[NodeId(msg.SenderID)] = time.Now()
	wm.mutex.Unlock()

	switch msg.Type {
	case Heartbeat:
		// Respond to heartbeat
		ackMsg := NewHeartbeatAckMessage(string(wm.config.CurrentNodeID))
		wm.sendMessage(addr, ackMsg)
	case HeartbeatAck:
		// Heartbeat acknowledged
	case Election:
		senderID, _ := strconv.Atoi(msg.SenderID)
		wm.handleElection(senderID)
	case Coordinator:
		senderID, _ := strconv.Atoi(msg.SenderID)
		wm.handleCoordinator(senderID)
	case Broadcast:
		wm.logger.Printf("Received broadcast from leader %s: %s", msg.SenderID, msg.Payload)
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
	wm.logger.Printf("Starting election - initiating Bully Algorithm")
	higherPeers := []NodeId{}
	for id := range wm.peers {
		if id > wm.config.CurrentNodeID {
			higherPeers = append(higherPeers, id)
		}
	}

	if len(higherPeers) == 0 {
		wm.logger.Printf("Election: No higher ID peers found, becoming leader")
		// No higher ID peers, become leader
		wm.becomeLeader()
		return
	}

	wm.logger.Printf("Election: Sending election messages to higher ID peers: %v", higherPeers)
	// Send election messages to higher ID peers
	for _, id := range higherPeers {
		if addr, ok := wm.peers[id]; ok {
			msg := NewElectionMessage(string(id))
			wm.sendMessage(addr, msg)
			wm.logger.Printf("Election: Sent election message to node %s", id)
		}
	}

	wm.logger.Printf("Election: Waiting %v for responses from higher ID peers", wm.config.Timeout)
	// Wait for responses
	time.Sleep(wm.config.Timeout)

	wm.mutex.Lock()
	if !wm.isLeader {
		wm.logger.Printf("Election: No responses received, becoming leader")
		// No response, become leader
		wm.becomeLeader()
	} else {
		wm.logger.Printf("Election: Already received coordinator message, election complete")
	}
	wm.mutex.Unlock()
}

// handleElection handles incoming election messages
func (wm *WatchMesh) handleElection(senderID int) {
	wm.logger.Printf("Election: Received election message from node %d", senderID)
	senderNodeID := NodeId(strconv.Itoa(senderID))
	if senderNodeID < wm.config.CurrentNodeID {
		wm.logger.Printf("Election: Responding to lower ID node %d with coordinator message", senderID)
		// Respond to lower ID node
		if addr, ok := wm.peers[senderNodeID]; ok {
			msg := NewCoordinatorMessage(string(wm.config.CurrentNodeID))
			wm.sendMessage(addr, msg)
		}
		// Start own election if not already
		if !wm.isLeader {
			wm.logger.Printf("Election: Starting own election as response to lower ID node")
			wm.startElection()
		}
	} else {
		wm.logger.Printf("Election: Ignoring election message from higher/equal ID node %d", senderID)
	}
}

// handleCoordinator handles incoming coordinator messages
func (wm *WatchMesh) handleCoordinator(senderID int) {
	wm.logger.Printf("Election: Received coordinator message from node %d", senderID)
	wm.mutex.Lock()
	wm.leaderID = NodeId(strconv.Itoa(senderID))
	wm.isLeader = false
	wm.mutex.Unlock()
	wm.logger.Printf("Election: New leader elected - node %d", senderID)
}

// becomeLeader sets this node as the leader
func (wm *WatchMesh) becomeLeader() {
	wm.logger.Printf("Election: Becoming leader - node %s elected", wm.config.CurrentNodeID)
	wm.mutex.Lock()
	wm.isLeader = true
	wm.leaderID = wm.config.CurrentNodeID
	wm.mutex.Unlock()
	wm.logger.Printf("Election: Broadcasting coordinator message to all peers")

	// Broadcast to all peers
	for id, addr := range wm.peers {
		msg := NewCoordinatorMessage(string(wm.config.CurrentNodeID))
		wm.sendMessage(addr, msg)
		wm.logger.Printf("Election: Sent coordinator message to node %s", id)
	}

	// Start leader tasks
	wm.logger.Printf("Election: Starting leader tasks")
	go wm.leaderTask()
}

// leaderTask performs leader-specific tasks
func (wm *WatchMesh) leaderTask() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		wm.mutex.Lock()
		if wm.isLeader {
			wm.logger.Printf("Leader broadcasting status")
			for _, addr := range wm.peers {
				msg := NewBroadcastMessage(string(wm.config.CurrentNodeID), "Leader status update")
				wm.sendMessage(addr, msg)
			}
		}
		wm.mutex.Unlock()
	}
}
