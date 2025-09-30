package filters

import (
	"common/middleware"
	"fmt"
)

const (
	ERROR_CHANNEL_BUFFER_SIZE = 20
	SINGLE_ITEM_BUFFER_LEN    = 1

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

func prepareQuery1InputQueues(rabbitConn *middleware.RabbitConnection) error {
	middlewareHandler, err := middleware.NewMiddlewareHandler(rabbitConn)
	if err != nil {
		return fmt.Errorf("failed to create middleware handler: %w", err)
	}
	// Declare and bind for Query 1
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
