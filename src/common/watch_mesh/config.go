package watch_mesh

import "time"

// Config holds the configuration for a distributed node
type WatchMeshConfig struct {
	CurrentNodeID            NodeId
	CurrentNodeIDNum         int
	WatchMeshPort            int
	PeerAddresses            []string
	NodeType                 string
	HeartbeatInterval        time.Duration
	HeartbeatTimeout         time.Duration
	AddressResolvingRetries  int
	AddressResolvingInterval time.Duration
	ShowHeartbeatLogs        bool
	MaxResurrectionAttempts  int
	RandomSeedForJitter      int64
	CrasherEnabled           bool
}

// NewWatchMeshConfig creates a new WatchMeshConfig with the provided parameters
func NewWatchMeshConfig(
	currentNodeID string,
	currentNodeIDNum int,
	watchMeshPort int,
	peerAddresses []string,
	heartbeatInterval time.Duration,
	heartbeatTimeout time.Duration,
	addressResolvingRetries int,
	addressResolvingInterval time.Duration,
	showHeartbeatLogs bool,
	nodeType string,
	maxResurrectionAttempts int,
	randomSeedForJitter int64,
	crasherEnabled bool,
) WatchMeshConfig {

	return WatchMeshConfig{
		CurrentNodeID:            NodeId(currentNodeID),
		CurrentNodeIDNum:         currentNodeIDNum,
		WatchMeshPort:            watchMeshPort,
		PeerAddresses:            peerAddresses,
		HeartbeatInterval:        heartbeatInterval,
		HeartbeatTimeout:         heartbeatTimeout,
		AddressResolvingRetries:  addressResolvingRetries,
		AddressResolvingInterval: addressResolvingInterval,
		ShowHeartbeatLogs:        showHeartbeatLogs,
		NodeType:                 nodeType,
		MaxResurrectionAttempts:  maxResurrectionAttempts,
		RandomSeedForJitter:      randomSeedForJitter,
		CrasherEnabled:           crasherEnabled,
	}
}

type BasicWatchMeshConfig struct {
	Port                            int
	HeartbeatIntervalSeconds        float64
	HeartbeatTimeoutSeconds         float64
	AddressResolvingRetries         int
	AddressResolvingIntervalSeconds float64
	ShowHeartbeatLogs               bool
	MaxResurrectionAttempts         int
	RandomSeedForJitter             int64
	CrasherEnabled                  bool
}

// NewBasicWatchMeshConfig creates a new BasicWatchMeshConfig with the provided parameters
func NewBasicWatchMeshConfig(
	port int,
	heartbeatIntervalSeconds float64,
	heartbeatTimeoutSeconds float64,
	addressResolvingRetries int,
	addressResolvingIntervalSeconds float64,
	showHeartbeatLogs bool,
	maxResurrectionAttempts int,
	randomSeedForJitter int64,
	crasherEnabled bool,
) BasicWatchMeshConfig {

	return BasicWatchMeshConfig{
		Port:                            port,
		HeartbeatIntervalSeconds:        heartbeatIntervalSeconds,
		HeartbeatTimeoutSeconds:         heartbeatTimeoutSeconds,
		AddressResolvingRetries:         addressResolvingRetries,
		AddressResolvingIntervalSeconds: addressResolvingIntervalSeconds,
		ShowHeartbeatLogs:               showHeartbeatLogs,
		MaxResurrectionAttempts:         maxResurrectionAttempts,
		RandomSeedForJitter:             randomSeedForJitter,
		CrasherEnabled:                  crasherEnabled,
	}
}
