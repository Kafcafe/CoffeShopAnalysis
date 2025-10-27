package middleware

import (
	"encoding/json"
	"fmt"
)

const (
	RESULTS_REQUEST_TYPE_GATHER RequestType = iota
	RESULTS_REQUEST_TYPE_CLEAR
	QUERY_ID_NOT_SET = -1
)

type RequestType int

func ToBytes(v any) ([]byte, error) {
	msgBytes, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("problem while marshalling: %w", err)
	}
	return msgBytes, nil
}

func FromBytes[T any](data []byte) (*T, error) {
	var msg T
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("failed deserialization: %w", err)
	}
	return &msg, nil
}

// Message
type Message struct {
	DataType     string
	ClientId     string
	Payload      []string
	IsEof        bool
	TotalEmitted int
	QueryId      int
}

func NewMessage(dataType, clientId string, payload []string, isEof bool, queryId int) *Message {
	return &Message{
		DataType: dataType,
		ClientId: clientId,
		Payload:  payload,
		IsEof:    isEof,
		QueryId:  queryId,
	}
}

func NewMessageFromBytes(msgBytes []byte) (*Message, error) {
	return FromBytes[Message](msgBytes)
}

func (m *Message) ToBytes() ([]byte, error) {
	return ToBytes(m)
}

// MessageGrouped
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
		DataType: dataType,
		ClientId: clientId,
		Payload:  payload,
		IsEof:    isEof,
		QueryId:  queryId,
	}
}

func (m *MessageGrouped) ToMessage() *Message {
	return &Message{
		DataType:     m.DataType,
		ClientId:     m.ClientId,
		IsEof:        m.IsEof,
		TotalEmitted: m.TotalEmitted,
		QueryId:      m.QueryId,
	}
}

func NewMessageGroupedFromBytes(msgBytes []byte) (*MessageGrouped, error) {
	return FromBytes[MessageGrouped](msgBytes)
}

func (m *MessageGrouped) ToBytes() ([]byte, error) {
	return ToBytes(m)
}

// MessageResultsRequest
type MessageResultsRequest struct {
	Origin      string // Who is requesting the results
	QueueName   string // To which queue the results should be sent
	ClientId    string // Client ID of the original data stream
	DataType    string // Data type of the original data stream
	RequestType RequestType
}

func NewMessageResultsRequest(origin, queueName, clientId, dataType string) *MessageResultsRequest {
	return &MessageResultsRequest{
		Origin:    origin,
		QueueName: queueName,
		ClientId:  clientId,
		DataType:  dataType,
	}
}

func NewGatherResultsRequest(origin, queueName, clientId, dataType string) *MessageResultsRequest {
	message := NewMessageResultsRequest(origin, queueName, clientId, dataType)
	message.RequestType = RESULTS_REQUEST_TYPE_GATHER
	return message
}
func NewClearResultsRequest(origin, queueName, clientId, dataType string) *MessageResultsRequest {
	message := NewMessageResultsRequest(origin, queueName, clientId, dataType)
	message.RequestType = RESULTS_REQUEST_TYPE_CLEAR
	return message
}

func (m *MessageResultsRequest) ToBytes() ([]byte, error) {
	return ToBytes(m)
}

func NewMessageResultsRequestFromBytes(msgBytes []byte) (*MessageResultsRequest, error) {
	return FromBytes[MessageResultsRequest](msgBytes)
}

// MessageResultsResponse
type MessageResultsResponse struct {
	Origin         string              // Who is sending the results
	ClientId       string              // Client ID of the original data stream
	DataType       string              // Data type of the original data stream
	Processed      int                 // Number of processed items
	Emitted        int                 // Number of emitted items
	Payload        []string            // The actual results
	GroupedPayload map[string][]string // The actual results in grouped form
	RequestType    RequestType         // Type of the original request
}

func (m *MessageResultsResponse) ToBytes() ([]byte, error) {
	return ToBytes(m)
}

func NewMessageResultsResponseFromBytes(msgBytes []byte) (*MessageResultsResponse, error) {
	return FromBytes[MessageResultsResponse](msgBytes)
}
