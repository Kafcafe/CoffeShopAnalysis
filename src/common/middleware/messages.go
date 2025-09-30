package middleware

import (
	"encoding/json"
	"fmt"
)

type Message struct {
	DataType string
	ClientId string
	Payload  []string
	IsEof    bool
}

func NewMessage(dataType, clientId string, payload []string, isEof bool) *Message {
	return &Message{
		DataType: dataType,
		ClientId: clientId,
		Payload:  payload,
		IsEof:    isEof,
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

func (m *Message) IsFromSameStream(other *EofMessage) bool {
	if other == nil {
		return false
	}
	return m.DataType == other.DataType && m.ClientId == other.ClientId
}

type EofMessage struct {
	DataType        string
	ClientId        string
	ImmediateSource string
	Origin          string
	IsAck           bool
}

func NewEofMessage(dataType, clientId, immediateSource, origin string, isAck bool) *EofMessage {
	return &EofMessage{
		DataType:        dataType,
		ClientId:        clientId,
		ImmediateSource: immediateSource,
		Origin:          origin,
		IsAck:           isAck,
	}
}

func NewEofMessageFromBytes(msgBytes []byte) (*EofMessage, error) {
	var msg EofMessage
	err := json.Unmarshal(msgBytes, &msg)
	if err != nil {
		return nil, fmt.Errorf("failed message deserialization: %w", err)
	}

	return &msg, nil
}

func (m *EofMessage) ToBytes() ([]byte, error) {
	msgBytes, err := json.Marshal(m)
	if err != nil {
		return []byte{}, fmt.Errorf("problem while marshalling message of dataType %s: %w", m.DataType, err)
	}

	return msgBytes, nil
}
