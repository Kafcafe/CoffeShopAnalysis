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

func (m *MessageMiddlewareQueue) StartConsuming(onMessageCallback OnMessageCallback, errChan chan<- MessageMiddlewareError) {
	consumeChannel, err := m.channel.Consume(
		m.queueName, // queue
		"",          // consumer
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)

	if err != nil {
		middleware_logger.Errorf("failed to start consuming channel: %v", err)
		errChan <- MessageMiddlewareDisconnectedError
		return
	}
	m.consumeChannel = &consumeChannel

	go func() {
		for msg := range consumeChannel {
			err := onMessageCallback(msg)
			if err != nil {
				errChan <- MessageMiddlewareMessageError
			}
		}
	}()
}

func (m *MessageMiddlewareQueue) StopConsuming() (middlewareError MessageMiddlewareError) {
	err := m.channel.Cancel(
		"",    // Consumer
		false, // noWait
	)

	if err != nil {
		return MessageMiddlewareDisconnectedError
	}
	return MessageMiddlewareSuccess
}

func (m *MessageMiddlewareQueue) Send(message []byte) (middlewareError MessageMiddlewareError) {
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

	return MessageMiddlewareSuccess
}

func (m *MessageMiddlewareQueue) Close() (middlewareError MessageMiddlewareError) {
	err := m.channel.Close()
	if err != nil {
		return MessageMiddlewareCloseError
	}

	return MessageMiddlewareSuccess
}
