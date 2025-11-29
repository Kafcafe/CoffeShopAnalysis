package watch_mesh

import (
	"encoding/json"
	"fmt"
)

// MessageType defines the type of message being exchanged between nodes.
type MessageType int

const (
	// Heartbeat is sent to signal that a node is alive.
	Heartbeat MessageType = 0
	// HeartbeatAck is sent in response to a Heartbeat message.
	HeartbeatAck MessageType = 1

	// Election is sent to initiate a leader election process.
	Election MessageType = 2
	// ElectionOk is sent to acknowledge an election message and assert presence.
	ElectionOk MessageType = 3

	// Coordinator is sent by the new leader to announce its leadership.
	Coordinator MessageType = 4
	// CoordinatorAck is sent to acknowledge the new leader.
	CoordinatorAck MessageType = 5

	// LeaderDiscovery is sent to find the current leader of the mesh.
	LeaderDiscovery MessageType = 6
	// LeaderResponse is sent by the leader or a node knowing the leader in response to LeaderDiscovery.
	LeaderResponse MessageType = 7
)

// WatchMeshMessage represents a message exchanged between nodes in the watch mesh.
type WatchMeshMessage struct {
	Type     MessageType
	SenderID string
	Payload  string
}

// ToBytes serializes the WatchMeshMessage into a JSON byte slice.
func (m *WatchMeshMessage) ToBytes() ([]byte, error) {
	msgBytes, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("problem while marshalling: %w", err)
	}
	return msgBytes, nil
}

// WatchMeshMessageFromBytes deserializes a byte slice into a WatchMeshMessage.
func WatchMeshMessageFromBytes(data []byte) (*WatchMeshMessage, error) {
	var msg WatchMeshMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("failed deserialization: %w", err)
	}
	return &msg, nil
}

// NewHeartbeatMessage creates a new Heartbeat message.
func NewHeartbeatMessage(senderID string) *WatchMeshMessage {
	return &WatchMeshMessage{
		Type:     Heartbeat,
		SenderID: senderID,
	}
}

// NewHeartbeatAckMessage creates a new HeartbeatAck message.
func NewHeartbeatAckMessage(senderID string) *WatchMeshMessage {
	return &WatchMeshMessage{
		Type:     HeartbeatAck,
		SenderID: senderID,
	}
}

// NewElectionMessage creates a new Election message.
func NewElectionMessage(senderID string) *WatchMeshMessage {
	return &WatchMeshMessage{
		Type:     Election,
		SenderID: senderID,
	}
}

// NewElectionOkMessage creates a new ElectionOk message.
func NewElectionOkMessage(senderID string) *WatchMeshMessage {
	return &WatchMeshMessage{
		Type:     ElectionOk,
		SenderID: senderID,
	}
}

// NewCoordinatorMessage creates a new Coordinator message.
func NewCoordinatorMessage(senderID string) *WatchMeshMessage {
	return &WatchMeshMessage{
		Type:     Coordinator,
		SenderID: senderID,
	}
}

// NewCoordinatorAckMessage creates a new CoordinatorAck message.
func NewCoordinatorAckMessage(senderID string) *WatchMeshMessage {
	return &WatchMeshMessage{
		Type:     CoordinatorAck,
		SenderID: senderID,
	}
}

// NewLeaderDiscoveryMessage creates a new LeaderDiscovery message.
func NewLeaderDiscoveryMessage(senderID string) *WatchMeshMessage {
	return &WatchMeshMessage{
		Type:     LeaderDiscovery,
		SenderID: senderID,
	}
}

// NewLeaderResponseMessage creates a new LeaderResponse message with the leader's ID as payload.
func NewLeaderResponseMessage(senderID, leaderID string) *WatchMeshMessage {
	return &WatchMeshMessage{
		Type:     LeaderResponse,
		SenderID: senderID,
		Payload:  leaderID,
	}
}
