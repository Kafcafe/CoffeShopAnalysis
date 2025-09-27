package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

func NewMessageMiddlewareExchange(exchangeName string, routeKeys []string, channel MiddlewareChannel, consumeChannel ConsumeChannel) *MessageMiddlewareExchange {
	return &MessageMiddlewareExchange{
		exchangeName:   exchangeName,
		routeKeys:      routeKeys,
		amqpChannel:    channel,
		consumeChannel: consumeChannel,
	}
}

func (m *MessageMiddlewareExchange) StartConsuming(onMessageCallback onMessageCallback) MessageMiddlewareError {
	done := make(chan error, 1)
	for msg := range *m.consumeChannel {
		onMessageCallback(m.consumeChannel, done)
		err := <-done
		if err == nil {
			msg.Ack(false)
		}
	}
	return 0
}

func (m *MessageMiddlewareExchange) StopConsuming() (error MessageMiddlewareError) {
	err := m.amqpChannel.Cancel("", false)
	if err != nil {
		return MessageMiddlewareDisconnectedError
	}
	return 0
}

func (m *MessageMiddlewareExchange) Send(message []byte) (error MessageMiddlewareError) {
	for _, routeKey := range m.routeKeys {
		err := m.amqpChannel.Publish(
			m.exchangeName, // exchange
			routeKey,       // routing key
			false,          // mandatory
			false,          // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        message,
			},
		)
		if err != nil {
			return MessageMiddlewareMessageError
		}
	}
	return 0
}

func (m *MessageMiddlewareExchange) Close() (error MessageMiddlewareError) {
	err := m.amqpChannel.Close()
	if err != nil {
		return MessageMiddlewareCloseError
	}
	return 0
}

func (m *MessageMiddlewareExchange) Delete() (error MessageMiddlewareError) {
	return
}
