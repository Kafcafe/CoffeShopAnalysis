package filters

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
	ACTIVITY                  = 0
	OPT_IS_EOF_ACK            = "ACK"
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

func prepareQuery1InputQueues(rabbitConn *middleware.RabbitConnection) error {
	middlewareHandler, err := middleware.NewMiddlewareHandler(rabbitConn)
	if err != nil {
		return fmt.Errorf("failed to create middleware handler: %w", err)
	}
	// Declare and bind for Query 1

	err = middlewareHandler.DeclareExchange(middleware.EXCHANGE_NAME_DIRECT_TYPE, middleware.EXCHANGE_TYPE_DIRECT)
	if err != nil {
		return fmt.Errorf("failed to declare exchange in prepareQuery1InputQueues: %v", err)
	}

	routeKey := "transactions.year-hour-filtered.q1"
	_, err = middlewareHandler.DeclareQueue(routeKey)
	if err != nil {
		return fmt.Errorf("failed to declare queue %s: %v", routeKey, err)
	}

	err = middlewareHandler.BindQueue(routeKey, middleware.EXCHANGE_NAME_DIRECT_TYPE, "transactions.year-hour-filtered.all")
	if err != nil {
		return fmt.Errorf("failed to bind queue to exchange: %v", err)
	}

	return nil
}

func prepareQuery1OutputQueues(rabbitConn *middleware.RabbitConnection) error {
	middlewareHandler, err := middleware.NewMiddlewareHandler(rabbitConn)
	if err != nil {
		return fmt.Errorf("failed to create middleware handler: %w", err)
	}
	// Declare and bind for Query 1

	err = middlewareHandler.DeclareExchange(middleware.EXCHANGE_NAME_TOPIC_TYPE, middleware.EXCHANGE_TYPE_TOPIC)
	if err != nil {
		return fmt.Errorf("failed to declare exchange in prepareQuery1OutputQueues: %v", err)
	}

	routeKey := "results.q1"
	_, err = middlewareHandler.DeclareQueue(routeKey)
	if err != nil {
		return fmt.Errorf("failed to declare queue %s: %v", routeKey, err)
	}

	err = middlewareHandler.BindQueue(routeKey, middleware.EXCHANGE_NAME_DIRECT_TYPE, routeKey)
	if err != nil {
		return fmt.Errorf("failed to bind queue to exchange: %v", err)
	}

	return nil
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

	queueName := fmt.Sprintf("eof.filters.%s.%s", filterType, filterId)
	_, err = middlewareHandler.DeclareQueue(queueName)
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue %s: %v", queueName, err)
	}

	err = middlewareHandler.BindQueue(queueName, middleware.EXCHANGE_NAME_TOPIC_TYPE, fmt.Sprintf("eof.filters.%s.*", filterType))
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
