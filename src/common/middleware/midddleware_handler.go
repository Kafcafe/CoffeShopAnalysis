package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type MiddlewareHandler struct {
	RabbitConn *RabbitConnection
	Channel    MiddlewareChannel
}

func NewMiddlewareHandler(rabbitConn *RabbitConnection) (*MiddlewareHandler, error) {
	ch, err := rabbitConn.CreateNewChannel()
	if err != nil {
		return nil, err
	}

	return &MiddlewareHandler{
		RabbitConn: rabbitConn,
		Channel:    ch,
	}, nil
}

func (mh *MiddlewareHandler) Close() error {
	if err := mh.Channel.Close(); err != nil {
		return err
	}
	if err := mh.RabbitConn.Close(); err != nil {
		return err
	}
	return nil
}

func (mh *MiddlewareHandler) DeclareQueue(queueName string) (*amqp.Queue, error) {
	q, err := mh.Channel.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)

	return &q, err
}

func (mh *MiddlewareHandler) DeclareExchange(exchangeName string, exchangeType string) error {
	err := mh.Channel.ExchangeDeclare(
		exchangeName, // name
		exchangeType, // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	return err
}

func (mh *MiddlewareHandler) BindQueue(queueName, exchangeName, routingKey string) error {
	err := mh.Channel.QueueBind(
		queueName,    // queue name
		routingKey,   // routing key
		exchangeName, // exchange
		false,
		nil,
	)
	return err
}

func (mh *MiddlewareHandler) ConsumeQueue(queueName string) (<-chan amqp.Delivery, error) {
	msgs, err := mh.Channel.Consume(
		queueName, // queue
		"",        // consumer
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	return msgs, err
}
