package watch_mesh

import (
	"encoding/json"
	"fmt"
)

type MessageType int

const (
	Heartbeat    MessageType = 0
	HeartbeatAck MessageType = 1

	Election    MessageType = 2
	Coordinator MessageType = 3

	Broadcast  MessageType = 4
	ElectionOk MessageType = 5

	LeaderDiscovery MessageType = 6
	LeaderResponse  MessageType = 7
)

type WatchMeshMessage struct {
	Type     MessageType
	SenderID string
	Payload  string
}

func (m *WatchMeshMessage) ToBytes() ([]byte, error) {
	msgBytes, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("problem while marshalling: %w", err)
	}
	return msgBytes, nil
}

func FromBytes(data []byte) (*WatchMeshMessage, error) {
	var msg WatchMeshMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("failed deserialization: %w", err)
	}
	return &msg, nil
}

func NewHeartbeatMessage(senderID string) *WatchMeshMessage {
	return &WatchMeshMessage{
		Type:     Heartbeat,
		SenderID: senderID,
	}
}

func NewHeartbeatAckMessage(senderID string) *WatchMeshMessage {
	return &WatchMeshMessage{
		Type:     HeartbeatAck,
		SenderID: senderID,
	}
}

func NewElectionMessage(senderID string) *WatchMeshMessage {
	return &WatchMeshMessage{
		Type:     Election,
		SenderID: senderID,
	}
}

func NewElectionOkMessage(senderID string) *WatchMeshMessage {
	return &WatchMeshMessage{
		Type:     ElectionOk,
		SenderID: senderID,
	}
}

func NewCoordinatorMessage(senderID string) *WatchMeshMessage {
	return &WatchMeshMessage{
		Type:     Coordinator,
		SenderID: senderID,
	}
}

func NewBroadcastMessage(senderID, payload string) *WatchMeshMessage {
	return &WatchMeshMessage{
		Type:     Broadcast,
		SenderID: senderID,
		Payload:  payload,
	}
}

func NewLeaderDiscoveryMessage(senderID string) *WatchMeshMessage {
	return &WatchMeshMessage{
		Type:     LeaderDiscovery,
		SenderID: senderID,
	}
}

func NewLeaderResponseMessage(senderID, leaderID string) *WatchMeshMessage {
	return &WatchMeshMessage{
		Type:     LeaderResponse,
		SenderID: senderID,
		Payload:  leaderID,
	}
}
