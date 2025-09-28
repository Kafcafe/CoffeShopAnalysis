package middleware

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	EXCHANGE_TYPE_TOPIC      = "topic"
	EXCHANGE_NAME_TOPIC_TYPE = "coffee-analysis-topic"

	EXCHANGE_DURABILITY = false
	QUEUE_DURABILITY    = false
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
		queueName,        // name
		QUEUE_DURABILITY, // durable
		false,            // delete when unused
		false,            // exclusive
		false,            // no-wait
		nil,              // arguments
	)

	return &q, err
}

func (mh *MiddlewareHandler) DeclareExchange(exchangeName, exchangeType string) error {
	err := mh.Channel.ExchangeDeclare(
		exchangeName,        // name
		exchangeType,        // type
		EXCHANGE_DURABILITY, // durable
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		nil,                 // arguments
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

func (mh *MiddlewareHandler) CreateTopicExchange(routeKey string) (*MessageMiddlewareExchange, error) {
	return mh.createExchange(EXCHANGE_NAME_TOPIC_TYPE, EXCHANGE_TYPE_TOPIC, routeKey)
}

func (mh *MiddlewareHandler) createExchange(exchangeName, exchangeType, routeKey string) (*MessageMiddlewareExchange, error) {
	err := mh.DeclareExchange(exchangeName, exchangeType)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange: %v", err)
	}

	_, err = mh.DeclareQueue(routeKey)
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue: %v", err)
	}

	err = mh.BindQueue(routeKey, exchangeName, routeKey)
	if err != nil {
		return nil, fmt.Errorf("failed to bind queue to exchange: %v", err)
	}

	return NewMessageMiddlewareExchange(exchangeName, routeKey, mh.Channel, nil), nil
}

func (mh *MiddlewareHandler) CreateQueue(queueName string) (*MessageMiddlewareQueue, error) {
	_, err := mh.DeclareQueue(queueName)
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue: %v", err)
	}

	return NewMessageMiddlewareQueue(queueName, mh.Channel, nil), nil
}
