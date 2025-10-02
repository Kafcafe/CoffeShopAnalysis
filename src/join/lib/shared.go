package join

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

// func prepareQuery1InputQueues(rabbitConn *middleware.RabbitConnection) error {
// 	middlewareHandler, err := middleware.NewMiddlewareHandler(rabbitConn)
// 	if err != nil {
// 		return fmt.Errorf("failed to create middleware handler: %w", err)
// 	}
// 	// Declare and bind for Query 1
// 	routeKey := "transactions.year-hour-filtered.q1"
// 	_, err = middlewareHandler.DeclareQueue(routeKey)
// 	if err != nil {
// 		return fmt.Errorf("failed to declare queue %s: %v", routeKey, err)
// 	}

// 	err = middlewareHandler.BindQueue(routeKey, middleware.EXCHANGE_NAME_DIRECT_TYPE, "transactions.year-hour-filtered.all")
// 	if err != nil {
// 		return fmt.Errorf("failed to bind queue to exchange: %v", err)
// 	}

// 	return nil
// }

// func prepareQuery1OutputQueues(rabbitConn *middleware.RabbitConnection) error {
// 	middlewareHandler, err := middleware.NewMiddlewareHandler(rabbitConn)
// 	if err != nil {
// 		return fmt.Errorf("failed to create middleware handler: %w", err)
// 	}
// 	// Declare and bind for Query 1
// 	routeKey := "results.q1"
// 	_, err = middlewareHandler.DeclareQueue(routeKey)
// 	if err != nil {
// 		return fmt.Errorf("failed to declare queue %s: %v", routeKey, err)
// 	}

// 	err = middlewareHandler.BindQueue(routeKey, middleware.EXCHANGE_NAME_DIRECT_TYPE, routeKey)
// 	if err != nil {
// 		return fmt.Errorf("failed to bind queue to exchange: %v", err)
// 	}

// 	return nil
// }

func prepareEofQueue(rabbitConn *middleware.RabbitConnection, joinerType string, joinerId string) (*middleware.MessageMiddlewareQueue, error) {
	middlewareHandler, err := middleware.NewMiddlewareHandler(rabbitConn)
	if err != nil {
		return nil, fmt.Errorf("failed to create middleware handler: %w", err)
	}

	// Declare and bind for Query 2
	queueName := fmt.Sprintf("eof.join.%s.%s", joinerType, joinerId)
	_, err = middlewareHandler.DeclareQueue(queueName)
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue %s: %v", queueName, err)
	}

	err = middlewareHandler.BindQueue(queueName, middleware.EXCHANGE_NAME_TOPIC_TYPE, fmt.Sprintf("eof.join.%s.*", joinerType))
	if err != nil {
		return nil, fmt.Errorf("failed to bind queue to exchange: %v", err)
	}

	return middleware.NewMessageMiddlewareQueue(queueName, middlewareHandler.Channel, nil), nil
}

func prepareSideTableQueue(rabbitConn *middleware.RabbitConnection, queueName, routeKey string) (*middleware.MessageMiddlewareQueue, error) {
	middlewareHandler, err := middleware.NewMiddlewareHandler(rabbitConn)
	if err != nil {
		return nil, fmt.Errorf("failed to create middleware handler: %w", err)
	}

	// Declare and bind
	err = middlewareHandler.DeclareExchange(middleware.EXCHANGE_NAME_TOPIC_TYPE, middleware.EXCHANGE_TYPE_TOPIC)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange in prepareSideTableQueue: %v", err)
	}

	_, err = middlewareHandler.DeclareQueue(queueName)
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue %s: %v", queueName, err)
	}

	err = middlewareHandler.BindQueue(queueName, middleware.EXCHANGE_NAME_TOPIC_TYPE, routeKey)
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
