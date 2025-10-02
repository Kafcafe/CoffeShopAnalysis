package middleware

import (
	"common/logger"

	amqp "github.com/rabbitmq/amqp091-go"
)

var middleware_logger = logger.GetLoggerWithPrefix("[EXCHANGE]")

func NewMessageMiddlewareExchange(exchangeName string, routeKey string, channel MiddlewareChannel, consumeChannel ConsumeChannel) *MessageMiddlewareExchange {
	return &MessageMiddlewareExchange{
		exchangeName:   exchangeName,
		routeKey:       routeKey,
		channel:        channel,
		consumeChannel: consumeChannel,
	}
}

func (m *MessageMiddlewareExchange) StartConsuming(onMessageCallback OnMessageCallback, errChan chan<- MessageMiddlewareError) {
	consumeChannel, err := m.channel.Consume(
		m.routeKey, // queue
		"",         // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
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
				middleware_logger.Errorf("Error while processing message: %v", err)
			}
		}
	}()
}

func (m *MessageMiddlewareExchange) StopConsuming() (middlewareError MessageMiddlewareError) {
	err := m.channel.Cancel(
		"",    // Consumer
		false, // noWait
	)

	if err != nil {
		return MessageMiddlewareDisconnectedError
	}
	return MessageMiddlewareSuccess
}

func (m *MessageMiddlewareExchange) Send(message []byte) (middlewareError MessageMiddlewareError) {
	err := m.channel.Publish(
		m.exchangeName, // exchange
		m.routeKey,     // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        message,
		},
	)

	if err != nil {
		middleware_logger.Errorf("error in sending: %v", err)
		return MessageMiddlewareMessageError
	}
	return MessageMiddlewareSuccess
}

func (m *MessageMiddlewareExchange) Close() (middlewareError MessageMiddlewareError) {
	if m.channel.IsClosed() {
		return MessageMiddlewareSuccess
	}
	err := m.channel.Close()
	if err != nil {
		return MessageMiddlewareCloseError
	}

	return MessageMiddlewareSuccess
}
