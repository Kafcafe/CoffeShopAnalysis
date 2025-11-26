package filters

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

type MiddlewareHandlers struct {
	prevStageSub               middleware.MessageMiddlewareQueue
	nextStagePubs              map[string]middleware.MessageMiddlewareExchange
	broadcastResultsRequestPub middleware.MessageMiddlewareExchange
	broadcastResultsRequestSub middleware.MessageMiddlewareQueue
}

func (f *FilterGenericWorker) createExchangeHandlers() error {

	mh, err := middleware.NewMiddlewareHandler(f.middlewareHandler.RabbitConn)
	if err != nil {
		return fmt.Errorf("failed to create middleware handler: %v", err)
	}

	_, err = mh.CreateDirectExchangeStandalone(f.conf.prevStageSub)
	if err != nil {
		return fmt.Errorf("error creating exchange handler for %s: %v", f.conf.prevStageSub, err)
	}
	// this name is just for identification purposes
	prevStageSubQueueName := f.conf.prevStageSub + "." + f.conf.ofType
	prevStageSub, err := mh.CreateQueue(prevStageSubQueueName)
	if err != nil {
		return fmt.Errorf("error creating queue handler for %s: %v", prevStageSubQueueName, err)
	}

	err = mh.BindQueue(prevStageSubQueueName, middleware.EXCHANGE_NAME_DIRECT_TYPE, f.conf.prevStageSub)
	if err != nil {
		return fmt.Errorf("error preparing queue for transactions: %v", err)
	}

	// Prepare next stage publishing handlers

	nextStagePubs := make(map[string]middleware.MessageMiddlewareExchange)
	for datatype, routeKey := range f.conf.nextStagePubs {
		f.log.Infof("Next stage publishing for datatype %s on routeKey %s", datatype, routeKey)
		exchange, err := mh.CreateDirectExchangeStandalone(routeKey)
		if err != nil {
			return fmt.Errorf("error creating exchange handler for %s: %v", routeKey, err)
		}
		nextStagePubs[datatype] = *exchange
	}

	f.log.Infof("Setting up results request Exchange for filter %s", f.conf.id)
	broadcastResultsRequestExchangeName := fmt.Sprintf("filter.%s.results.request", f.conf.ofType)
	broadcastResultsRequestPub, err := mh.CreateFanoutExchangeStandalone(broadcastResultsRequestExchangeName)
	if err != nil {
		return fmt.Errorf("error creating exchange handler for %s: %v", broadcastResultsRequestExchangeName, err)
	}

	f.log.Infof("Setting up results request SUB for filter %s", f.conf.id)
	broadcastResultsRequestSubQueueName := fmt.Sprintf("filter.%s.results.request.%s", f.conf.ofType, f.conf.id)
	broadcastResultsRequestSub, err := mh.CreateQueue(broadcastResultsRequestSubQueueName)
	if err != nil {
		return fmt.Errorf("error creating results request queue for %s: %v", broadcastResultsRequestSubQueueName, err)
	}
	err = mh.BindQueue(broadcastResultsRequestSubQueueName, broadcastResultsRequestExchangeName, "")

	if err != nil {
		return fmt.Errorf("error preparing results request queue for %s: %v", f.conf.ofType, err)
	}

	f.middlewareHandlers = MiddlewareHandlers{
		prevStageSub:               *prevStageSub,
		nextStagePubs:              nextStagePubs,
		broadcastResultsRequestPub: *broadcastResultsRequestPub,
		broadcastResultsRequestSub: *broadcastResultsRequestSub,
	}
	return nil
}

func (f *FilterGenericWorker) getNextStagePub(clientId ClientId, dataType DataType) (middleware.MessageMiddlewareExchange, error) {
	if f.conf.ofType != FILTER_TYPE_AMOUNT {
		return f.middlewareHandlers.nextStagePubs[dataType], nil
	}
	nextStagePub, exists := f.middlewareHandlers.nextStagePubs[clientId]
	if !exists {
		routeKey := fmt.Sprintf("results.%s", clientId)
		f.log.Infof("Next stage publishing for datatype %s on routeKey %s", dataType, routeKey)
		exchange, err := f.middlewareHandler.CreateDirectExchangeStandalone(routeKey)
		if err != nil {
			return middleware.MessageMiddlewareExchange{}, fmt.Errorf("error creating exchange handler for %s: %v", routeKey, err)
		}
		f.middlewareHandlers.nextStagePubs[clientId] = *exchange
		nextStagePub = *exchange
	}
	return nextStagePub, nil
}

func (f *FilterGenericWorker) SendToQueue(queueName string, message []byte) middleware.MessageMiddlewareError {
	// declare queue many to one (many publishers one consumer)
	mh, err := middleware.NewMiddlewareHandler(f.middlewareHandler.RabbitConn)
	if err != nil {
		f.log.Errorf("Failed to create middleware handler: %v", err)
		return middleware.MessageMiddlewareMessageError
	}
	queue, err := mh.CreateQueue(queueName)

	if err != nil {
		f.log.Errorf("Failed to declare queue %s: %v", queueName, err)
		return middleware.MessageMiddlewareMessageError
	}
	sendErr := queue.Send(message)
	if sendErr != middleware.MessageMiddlewareSuccess {
		f.log.Errorf("Failed to send message to queue %s: %v", queueName, sendErr)
		return middleware.MessageMiddlewareMessageError
	}
	return middleware.MessageMiddlewareSuccess
}

func (f *FilterGenericWorker) sendNextStage(msgToSend middleware.Message) error {
	nextStagePub, err := f.getNextStagePub(msgToSend.ClientId, msgToSend.DataType)
	if err != nil {
		return fmt.Errorf("received unprocessabble message in sendNextStage of type %s", msgToSend.DataType)
	}
	msgBytes, err := msgToSend.ToBytes()
	if err != nil {
		return err
	}
	f.clientsStatsMutex.Lock()
	nextStagePub.Send(msgBytes)
	f.clientsStatsMutex.Unlock()
	return nil
}

func answerMessage(ackType int, message amqp.Delivery) {
	switch ackType {
	case ACK:
		message.Ack(false)
	case NACK_REQUEUE:
		message.Nack(false, true)
	case NACK_DISCARD:
		message.Nack(false, false)
	}
}
