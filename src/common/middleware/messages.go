package middleware

import (
	"encoding/json"
	"fmt"
)

type Message struct {
	DataType string
	ClientId string
	Payload  []string
}

func NewMessage(dataType, clientId string, payload []string) *Message {
	return &Message{
		DataType: dataType,
		ClientId: clientId,
		Payload:  payload,
	}
}

func NewMessageFromBytes(msgBytes []byte) (*Message, error) {
	var msg Message
	err := json.Unmarshal(msgBytes, &msg)
	if err != nil {
		return nil, fmt.Errorf("failed message deserialization: %w", err)
	}

	return &msg, nil
}

func (m *Message) ToBytes() ([]byte, error) {
	msgBytes, err := json.Marshal(m)
	if err != nil {
		return []byte{}, fmt.Errorf("problem while marshalling message of dataType %s: %w", m.DataType, err)
	}

	return msgBytes, nil
}
