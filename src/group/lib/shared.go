package group

import (
	"common/middleware"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ERROR_CHANNEL_BUFFER_SIZE = 20
	SINGLE_ITEM_BUFFER_LEN    = 1

	ACK          = 0
	NACK_REQUEUE = 1
	NACK_DISCARD = 2

	THERE_IS_PREVIOUS_MESSAGE = 0
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

func answerMessage(ackType int, message amqp.Delivery) {
	switch ackType {
	case ACK:
	case NACK_REQUEUE:
		message.Nack(false, true)
	case NACK_DISCARD:
		message.Nack(false, false)
	}
}

func prepareEofQueue(rabbitConn *middleware.RabbitConnection, filterType string, filterId string) (*middleware.MessageMiddlewareQueue, error) {
	middlewareHandler, err := middleware.NewMiddlewareHandler(rabbitConn)
	if err != nil {
		return nil, fmt.Errorf("failed to create middleware handler: %w", err)
	}

	// Declare and bind for Query 1
	err = middlewareHandler.DeclareExchange(middleware.EXCHANGE_NAME_TOPIC_TYPE, middleware.EXCHANGE_TYPE_TOPIC)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange in prepareEofQueue: %v", err)
	}

	queueName := fmt.Sprintf("eof.group.%s.%s", filterType, filterId)
	_, err = middlewareHandler.DeclareQueue(queueName)
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue %s: %v", queueName, err)
	}

	err = middlewareHandler.BindQueue(queueName, middleware.EXCHANGE_NAME_TOPIC_TYPE, fmt.Sprintf("eof.group.%s.*", filterType))
	if err != nil {
		return nil, fmt.Errorf("failed to bind queue to exchange: %v", err)
	}

	return middleware.NewMessageMiddlewareQueue(queueName, middlewareHandler.Channel, nil), nil
}

func prepareInputQueues(rabbitConn *middleware.RabbitConnection, groupType string) error {
	middlewareHandler, err := middleware.NewMiddlewareHandler(rabbitConn)
	if err != nil {
		return fmt.Errorf("failed to create middleware handler: %w", err)
	}
	// Declare and bind for Query 1
	routeKey := fmt.Sprintf("transactions.transactions.%s", groupType)
	_, err = middlewareHandler.DeclareQueue(routeKey)
	if err != nil {
		return fmt.Errorf("failed to declare queue %s: %v", routeKey, err)
	}

	err = middlewareHandler.BindQueue(routeKey, middleware.EXCHANGE_NAME_TOPIC_TYPE, "transactions.transactions.all")
	if err != nil {
		return fmt.Errorf("failed to bind queue to exchange: %v", err)
	}

	return nil
}
