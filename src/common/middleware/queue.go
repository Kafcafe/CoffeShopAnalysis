package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

func NewMessageMiddlewareQueue(queueName string, channel MiddlewareChannel, consumeChannel ConsumeChannel) *MessageMiddlewareQueue {
	return &MessageMiddlewareQueue{
		queueName:      queueName,
		channel:        channel,
		consumeChannel: consumeChannel,
	}
}

func (m *MessageMiddlewareQueue) StartConsuming(onMessageCallback onMessageCallback) MessageMiddlewareError {
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

func (m *MessageMiddlewareQueue) StopConsuming() (error MessageMiddlewareError) {
	return 0
}

func (m *MessageMiddlewareQueue) Send(message []byte) (error MessageMiddlewareError) {
	err := m.channel.Publish(
		"",          // exchange
		m.queueName, // routing key (queue name)
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        message,
		},
	)
	if err != nil {
		return MessageMiddlewareMessageError
	}
	return 0
}

func (m *MessageMiddlewareQueue) Close() (error MessageMiddlewareError) {
	err := m.channel.Close()
	if err != nil {
		return MessageMiddlewareCloseError
	}
	return 0
}
