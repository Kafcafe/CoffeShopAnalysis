package topk

import (
	"common/middleware"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ACK          = 0
	NACK_REQUEUE = 1
	NACK_DISCARD = 2
)

func createExchangeHandler(rabbitConn *middleware.RabbitConnection, routeKey string, exchangeType string) (*middleware.MessageMiddlewareExchange, error) {
	middlewareHandler, err := middleware.NewMiddlewareHandler(rabbitConn)
	if err != nil {
		return nil, fmt.Errorf("failed to create middleware handler: %w", err)
	}

	if exchangeType == middleware.EXCHANGE_TYPE_DIRECT {
		return middlewareHandler.CreateDirectExchange(routeKey)
	}
	if exchangeType == middleware.EXCHANGE_TYPE_FANOUT {
		return middlewareHandler.CreateFanoutExchange(routeKey)
	}
	return middlewareHandler.CreateTopicExchange(routeKey)
}

func prepareEofQueue(rabbitConn *middleware.RabbitConnection, topKId string) (*middleware.MessageMiddlewareQueue, error) {
	middlewareHandler, err := middleware.NewMiddlewareHandler(rabbitConn)
	if err != nil {
		return nil, fmt.Errorf("failed to create middleware handler: %w", err)
	}

	err = middlewareHandler.DeclareExchange(middleware.EXCHANGE_NAME_TOPIC_TYPE, middleware.EXCHANGE_TYPE_TOPIC)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange in prepareEofQueue: %v", err)
	}

	queueName := fmt.Sprintf("eof.topk.%s", topKId)
	_, err = middlewareHandler.DeclareQueue(queueName)
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue %s: %v", queueName, err)
	}

	err = middlewareHandler.BindQueue(queueName, middleware.EXCHANGE_NAME_TOPIC_TYPE, "eof.topk.*")
	if err != nil {
		return nil, fmt.Errorf("failed to bind queue to exchange: %v", err)
	}

	return middleware.NewMessageMiddlewareQueue(queueName, middlewareHandler.Channel, nil), nil
}

func answerMessage(ackType int, message amqp.Delivery) {
	switch ackType {
	case ACK:
	case NACK_REQUEUE:
		message.Nack(false, true)
	case NACK_DISCARD:
		message.Nack(false, false)
	}
}
