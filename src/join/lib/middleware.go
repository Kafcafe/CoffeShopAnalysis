package join

import (
	"common/middleware"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ACK                 = 0
	NACK_REQUEUE        = 1
	NACK_DISCARD        = 2
	REQUEUE_PROBABILITY = 0.2
)

type JoinMiddlewareHandlers struct {
	prevStageSub               middleware.MessageMiddlewareQueue
	sideTableSub               middleware.MessageMiddlewareQueue
	nextStagePubs              map[string]middleware.MessageMiddlewareExchange
	privateQueueSub            middleware.MessageMiddlewareQueue
	privateQueuesPub           map[int]*middleware.MessageMiddlewareQueue
	broadcastResultsRequestPub middleware.MessageMiddlewareExchange
	broadcastResultsRequestSub middleware.MessageMiddlewareQueue
}

func (mh *JoinMiddlewareHandlers) Shutdown() {
	mh.prevStageSub.Close()
	mh.sideTableSub.Close()
	for _, nextStagePub := range mh.nextStagePubs {
		nextStagePub.Close()
	}
}

func (j *JoinGenericWorker) createExchangeHandlers() error {
	// PREV STAGE SUB
	_, err := j.middlewareHandler.CreateDirectExchangeStandalone(j.conf.prevStageSub)
	if err != nil {
		return fmt.Errorf("error creating exchange handler for results.q2: %v", err)
	}

	// this name is just for identification purposes
	prevStageSubQueueName := j.conf.prevStageSub + "." + j.conf.ofType
	prevStageSub, err := j.middlewareHandler.CreateQueue(prevStageSubQueueName)
	if err != nil {
		return fmt.Errorf("error creating queue handler for %s: %v", prevStageSubQueueName, err)
	}

	err = j.middlewareHandler.BindQueue(prevStageSubQueueName, middleware.EXCHANGE_NAME_DIRECT_TYPE, j.conf.prevStageSub)
	if err != nil {
		return fmt.Errorf("error preparing queue for transactions: %v", err)
	}

	// SIDE TABLE SUB
	_, err = j.middlewareHandler.CreateDirectExchangeStandalone(j.conf.sideTableSub)
	if err != nil {
		return fmt.Errorf("error creating exchange handler for %s: %v", j.conf.sideTableSub, err)
	}

	queueName := fmt.Sprintf("%s.%s.%s", j.conf.sideTableSub, j.conf.ofType, j.conf.id)
	sideTableSub, err := j.middlewareHandler.CreateQueue(queueName)
	if err != nil {
		return fmt.Errorf("error creating queue handler for %s: %v", queueName, err)
	}

	err = j.middlewareHandler.BindQueue(queueName, middleware.EXCHANGE_NAME_DIRECT_TYPE, j.conf.sideTableSub)
	if err != nil {
		return fmt.Errorf("error preparing side table queue for %s: %v", j.conf.ofType, err)
	}

	privateQueueName := fmt.Sprintf("join.%s.private.%d", j.conf.ofType, j.conf.idNum)
	privateQueueSub, err := j.middlewareHandler.CreateQueue(privateQueueName)
	if err != nil {
		return fmt.Errorf("error creating private queue for join %s: %v", j.conf.id, err)
	}

	privateQueuesPub := make(map[int]*middleware.MessageMiddlewareQueue)
	for i := range j.conf.count {
		privateQueuePubName := fmt.Sprintf("join.%s.private.%d", j.conf.ofType, i+1)
		queue, err := j.middlewareHandler.CreateQueue(privateQueuePubName)
		if err != nil {
			return fmt.Errorf("error creating private queue PUB for join %s: %v", j.conf.id, err)
		}
		privateQueuesPub[i+1] = queue
	}

	// NEXT STAGE PUB
	nextStagePubs := make(map[string]middleware.MessageMiddlewareExchange)
	nextStagePub, err := j.middlewareHandler.CreateDirectExchangeStandalone(j.conf.nextStagePubs[j.conf.ofType])
	if err != nil {
		return fmt.Errorf("error creating exchange handler for results.q2: %v", err)
	}
	nextStagePubs[j.conf.ofType] = *nextStagePub

	// RESULTS REQUEST PUB/SUB
	j.log.Infof("Setting up results request Exchange for join %s", j.conf.id)
	broadcastResultsRequestExchangeName := fmt.Sprintf("join.%s.results.request", j.conf.ofType)
	broadcastResultsRequestPub, err := j.middlewareHandler.CreateFanoutExchangeStandalone(broadcastResultsRequestExchangeName)
	if err != nil {
		return fmt.Errorf("error creating exchange handler for %s: %v", broadcastResultsRequestExchangeName, err)
	}

	j.log.Infof("Setting up results request SUB for join %s", j.conf.id)
	broadcastResultsRequestSubQueueName := fmt.Sprintf("join.%s.results.request.%s", j.conf.ofType, j.conf.id)
	broadcastResultsRequestSub, err := j.middlewareHandler.CreateQueue(broadcastResultsRequestSubQueueName)
	if err != nil {
		return fmt.Errorf("error creating results request queue for %s: %v", broadcastResultsRequestSubQueueName, err)
	}
	err = j.middlewareHandler.BindQueue(broadcastResultsRequestSubQueueName, broadcastResultsRequestExchangeName, "")

	if err != nil {
		return fmt.Errorf("error preparing results request queue for %s: %v", j.conf.ofType, err)
	}

	j.middlewareHandlers = JoinMiddlewareHandlers{
		prevStageSub:               *prevStageSub,
		sideTableSub:               *sideTableSub,
		nextStagePubs:              nextStagePubs,
		privateQueueSub:            *privateQueueSub,
		privateQueuesPub:           privateQueuesPub,
		broadcastResultsRequestPub: *broadcastResultsRequestPub,
		broadcastResultsRequestSub: *broadcastResultsRequestSub,
	}
	return nil
}

func (j *JoinGenericWorker) sendNextStage(msgToSend middleware.Message) (err error) {
	msgBytes, err := msgToSend.ToBytes()
	if err != nil {
		return err
	}

	var nextStagePub middleware.MessageMiddlewareExchange
	var exists bool
	var routeKey string

	if j.conf.ofType == JOIN_STORE_TYPE {
		nextStagePub, exists = j.middlewareHandlers.nextStagePubs[j.conf.ofType]
		if !exists {
			return fmt.Errorf("received unprocessabble message in sendNextStage of type %s", msgToSend.DataType)
		}
	} else {
		nextStagePub, exists = j.middlewareHandlers.nextStagePubs[msgToSend.ClientId]
		routeKey = fmt.Sprintf("results.%s", msgToSend.ClientId)
		if !exists {
			j.log.Infof("Next stage publishing for datatype %s on routeKey %s", msgToSend.DataType, routeKey)
			exchange, err := j.middlewareHandler.CreateDirectExchangeStandalone(routeKey)
			if err != nil {
				return fmt.Errorf("error creating exchange handler for %s: %v", routeKey, err)
			}
			j.middlewareHandlers.nextStagePubs[msgToSend.ClientId] = *exchange
			nextStagePub = *exchange
		}
	}

	j.middlewareMutex.Lock()
	nextStagePub.Send(msgBytes)
	j.middlewareMutex.Unlock()
	return nil
}

func (j *JoinGenericWorker) SendToQueue(queueName string, message []byte) middleware.MessageMiddlewareError {
	// declare queue many to one (many publishers one consumer)
	queue, err := j.middlewareHandler.CreateQueue(queueName)

	if err != nil {
		j.log.Errorf("Failed to declare queue %s: %v", queueName, err)
		return middleware.MessageMiddlewareMessageError
	}
	sendErr := queue.Send(message)
	if sendErr != middleware.MessageMiddlewareSuccess {
		j.log.Errorf("Failed to send message to queue %s: %v", queueName, sendErr)
		return middleware.MessageMiddlewareMessageError
	}
	return middleware.MessageMiddlewareSuccess
}

func (j *JoinGenericWorker) dispatchMessage(message amqp.Delivery) error {
	message_id, destination_id := middleware.GetMessageId(message.Body, j.conf.count)
	err := j.middlewareHandlers.privateQueuesPub[destination_id].SendWithId(message.Body, message_id)
	if err != middleware.MessageMiddlewareSuccess {
		answerMessage(NACK_REQUEUE, message)
		return fmt.Errorf("failed to dispatch message to private queue %d: %v", destination_id, err)
	}
	j.crasher.ThrowDiceAndForceExit("dispatch - dispatched - before ack")
	answerMessage(ACK, message)
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
