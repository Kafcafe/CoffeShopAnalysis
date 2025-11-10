package watch_mesh

import "time"

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
	ShowHeartbeatLogs        bool
}

// NewWatchMeshConfig creates a new WatchMeshConfig with the provided parameters
func NewWatchMeshConfig(
	currentNodeID string,
	watchMeshPort int,
	peerAddresses []string,
	heartbeatInterval time.Duration,
	heartbeatTimeout time.Duration,
	addressResolvingRetries int,
	addressResolvingInterval time.Duration,
	showHeartbeatLogs bool,
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
	}
}

type BasicWatchMeshConfig struct {
	Port                            int
	HeartbeatIntervalSeconds        float64
	HeartbeatTimeoutSeconds         float64
	AddressResolvingRetries         int
	AddressResolvingIntervalSeconds float64
	ShowHeartbeatLogs               bool
}

// NewBasicWatchMeshConfig creates a new BasicWatchMeshConfig with the provided parameters
func NewBasicWatchMeshConfig(
	port int,
	heartbeatIntervalSeconds float64,
	heartbeatTimeoutSeconds float64,
	addressResolvingRetries int,
	addressResolvingIntervalSeconds float64,
	showHeartbeatLogs bool,
) BasicWatchMeshConfig {

	return BasicWatchMeshConfig{
		Port:                            port,
		HeartbeatIntervalSeconds:        heartbeatIntervalSeconds,
		HeartbeatTimeoutSeconds:         heartbeatTimeoutSeconds,
		AddressResolvingRetries:         addressResolvingRetries,
		AddressResolvingIntervalSeconds: addressResolvingIntervalSeconds,
		ShowHeartbeatLogs:               showHeartbeatLogs,
	}
}
