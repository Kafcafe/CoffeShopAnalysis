package watch_mesh

import "time"

// WatchMeshConfig holds the configuration for the WatchMesh system.
// It includes settings for network communication, heartbeat mechanisms, and node identification.
type WatchMeshConfig struct {
	CurrentNodeID            NodeId
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
}

// NewWatchMeshConfig creates a new WatchMeshConfig with the provided parameters.
// It initializes the configuration with specific values for timeouts, retries, and node details.
func NewWatchMeshConfig(
	currentNodeID string,
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
) WatchMeshConfig {

	return WatchMeshConfig{
		CurrentNodeID:            NodeId(currentNodeID),
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
	}
}

// BasicWatchMeshConfig holds a simplified configuration structure, primarily used for
// parsing configuration from external sources (like JSON or YAML) before converting to WatchMeshConfig.
type BasicWatchMeshConfig struct {
	Port                            int
	HeartbeatIntervalSeconds        float64
	HeartbeatTimeoutSeconds         float64
	AddressResolvingRetries         int
	AddressResolvingIntervalSeconds float64
	ShowHeartbeatLogs               bool
	MaxResurrectionAttempts         int
	RandomSeedForJitter             int64
}

// NewBasicWatchMeshConfig creates a new BasicWatchMeshConfig with the provided parameters.
func NewBasicWatchMeshConfig(
	port int,
	heartbeatIntervalSeconds float64,
	heartbeatTimeoutSeconds float64,
	addressResolvingRetries int,
	addressResolvingIntervalSeconds float64,
	showHeartbeatLogs bool,
	maxResurrectionAttempts int,
	randomSeedForJitter int64,
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
	}
}
