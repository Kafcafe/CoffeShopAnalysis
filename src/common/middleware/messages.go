package middleware

type Message struct {
	DataType string
	ClientId string
	Payload  []byte
}

func NewMessage(dataType, clientId string, payload []byte) *Message {
	return &Message{
		DataType: dataType,
		ClientId: clientId,
		Payload:  payload,
	}
}
