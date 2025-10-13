package middleware

import (
	"encoding/json"
	"fmt"
)

type Message struct {
	DataType     string
	ClientId     string
	Payload      []string
	IsEof        bool
	TotalEmitted int
	QueryId      int
}

const (
	QUERY_ID_NOT_SET = -1
)

func NewMessage(dataType, clientId string, payload []string, isEof bool, queryId int) *Message {
	return &Message{
		DataType:     dataType,
		ClientId:     clientId,
		Payload:      payload,
		IsEof:        isEof,
		TotalEmitted: 0,
		QueryId:      queryId,
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

func (m *Message) IsFromSameStream(otherDataType string, otherClientId string) bool {
	return m.DataType == otherDataType && m.ClientId == otherClientId
}

//////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////

type EofMessage struct {
	DataType        string
	ClientId        string
	ImmediateSource string
	Origin          string
	IsAck           bool
	QueryId         int
}

func NewEofMessage(dataType, clientId, immediateSource, origin string, isAck bool, queryId int) *EofMessage {
	return &EofMessage{
		DataType:        dataType,
		ClientId:        clientId,
		ImmediateSource: immediateSource,
		Origin:          origin,
		IsAck:           isAck,
		QueryId:         queryId,
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

//////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////

type MessageGrouped struct {
	DataType     string
	ClientId     string
	Payload      map[string][]string
	IsEof        bool
	TotalEmitted int
	QueryId      int
}

func NewMessageGrouped(dataType, clientId string, payload map[string][]string, isEof bool, queryId int) *MessageGrouped {
	return &MessageGrouped{
		DataType:     dataType,
		ClientId:     clientId,
		Payload:      payload,
		IsEof:        isEof,
		TotalEmitted: 0,
		QueryId:      queryId,
	}
}

func (m *MessageGrouped) ToEmptyMessage() *Message {
	return &Message{
		DataType:     m.DataType,
		ClientId:     m.ClientId,
		Payload:      []string{},
		IsEof:        m.IsEof,
		TotalEmitted: m.TotalEmitted,
		QueryId:      m.QueryId,
	}
}

func (m *MessageGrouped) ToMessage() *Message {
	flatPayload := []string{}
	for _, values := range m.Payload {
		flatPayload = append(flatPayload, values...)
	}
	return &Message{
		DataType:     m.DataType,
		ClientId:     m.ClientId,
		Payload:      flatPayload,
		IsEof:        m.IsEof,
		TotalEmitted: m.TotalEmitted,
		QueryId:      m.QueryId,
	}
}

func NewMessageGroupedFromBytes(msgBytes []byte) (*MessageGrouped, error) {
	var msg MessageGrouped
	err := json.Unmarshal(msgBytes, &msg)
	if err != nil {
		return nil, fmt.Errorf("failed message deserialization: %w", err)
	}

	return &msg, nil
}

func (m *MessageGrouped) ToBytes() ([]byte, error) {
	msgBytes, err := json.Marshal(m)
	if err != nil {
		return []byte{}, fmt.Errorf("problem while marshalling message of dataType %s: %w", m.DataType, err)
	}

	return msgBytes, nil
}

func (m *MessageGrouped) IsFromSameStream(otherDataType string, otherClientId string) bool {
	return m.DataType == otherDataType && m.ClientId == otherClientId
}

//////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////

type EofMessageGrouped struct {
	DataType        string
	ClientId        string
	ImmediateSource string
	Origin          string
	IsAck           bool
	Payload         map[string][]string
	QueryId         int
}

func NewEofMessageGrouped(dataType, clientId, immediateSource, origin string, isAck bool, payload map[string][]string, queryId int) *EofMessageGrouped {
	return &EofMessageGrouped{
		DataType:        dataType,
		ClientId:        clientId,
		ImmediateSource: immediateSource,
		Origin:          origin,
		IsAck:           isAck,
		Payload:         payload,
		QueryId:         queryId,
	}
}

func NewEofMessageGroupedFromBytes(msgBytes []byte) (*EofMessageGrouped, error) {
	var msg EofMessageGrouped
	err := json.Unmarshal(msgBytes, &msg)
	if err != nil {
		return nil, fmt.Errorf("failed message deserialization: %w", err)
	}

	return &msg, nil
}

func (m *EofMessageGrouped) ToBytes() ([]byte, error) {
	msgBytes, err := json.Marshal(m)
	if err != nil {
		return []byte{}, fmt.Errorf("problem while marshalling message of dataType %s: %w", m.DataType, err)
	}

	return msgBytes, nil
}

// New eof method types

type MessageProcessed struct {
	DataType string
	ClientId string
	Emitted  bool
	QueryID  int
}

func NewMessageProcessed(dataType, clientId string, emitted bool, queryId int) *MessageProcessed {
	return &MessageProcessed{
		DataType: dataType,
		ClientId: clientId,
		Emitted:  emitted,
		QueryID:  queryId,
	}
}

func (m *MessageProcessed) ToBytes() ([]byte, error) {
	msgBytes, err := json.Marshal(m)
	if err != nil {
		return []byte{}, fmt.Errorf("problem while marshalling MessageProcessed of dataType %s: %w", m.DataType, err)
	}
	return msgBytes, nil
}

func NewMessageProcessedFromBytes(msgBytes []byte) (*MessageProcessed, error) {
	var msg MessageProcessed
	err := json.Unmarshal(msgBytes, &msg)
	if err != nil {
		return nil, fmt.Errorf("failed message deserialization: %w", err)
	}
	return &msg, nil
}
