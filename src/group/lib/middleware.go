package group

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

func (g *GroupByGenericWorker) createExchangeHandlers() error {
	// PREV STAGE SUB
	g.log.Infof("Creating exchange handler for previous stage subscription: %s", g.conf.prevStageSub)
	_, err := g.middlewareHandler.CreateDirectExchangeStandalone(g.conf.prevStageSub)
	if err != nil {
		return fmt.Errorf("error creating previous stage subscription exchange: %v", err)
	}

	// this name is just for identification purposes
	g.log.Infof("Creating queue handler for previous stage subscription: %s", g.conf.prevStageSub)
	prevStageQueueName := fmt.Sprintf("%s.%s", g.conf.prevStageSub, g.conf.ofType)
	prevStageSub, err := g.middlewareHandler.CreateQueue(prevStageQueueName)
	if err != nil {
		return fmt.Errorf("error creating queue for previous stage: %v", err)
	}

	g.log.Infof("Binding queue handler for previous stage subscription: %s", g.conf.prevStageSub)
	err = g.middlewareHandler.BindQueue(prevStageQueueName, middleware.EXCHANGE_NAME_DIRECT_TYPE, g.conf.prevStageSub)
	if err != nil {
		return fmt.Errorf("error binding queue for previous stage: %v", err)
	}

	privateQueueName := fmt.Sprintf("group.%s.private.%d", g.conf.ofType, g.conf.idNum)
	g.log.Infof("Creating private queue handler for group %s: %s", g.conf.id, privateQueueName)
	privateQueueSub, err := g.middlewareHandler.CreateQueue(privateQueueName)
	if err != nil {
		return fmt.Errorf("error creating private queue for group %s: %v", g.conf.id, err)
	}

	privateQueuesPub := make(map[int]*middleware.MessageMiddlewareQueue)
	for i := range g.conf.count {
		privateQueuePubName := fmt.Sprintf("group.%s.private.%d", g.conf.ofType, i+1)
		g.log.Infof("Creating private queue PUB handler for group %s: %s", g.conf.id, privateQueuePubName)
		queue, err := g.middlewareHandler.CreateQueue(privateQueuePubName)
		if err != nil {
			return fmt.Errorf("error creating private queue PUB for group %s: %v", g.conf.id, err)
		}
		privateQueuesPub[i+1] = queue
	}

	// NEXT STAGE PUB
	g.log.Infof("Creating exchange handler for next stage publication: %s", g.conf.nextStagePub)
	nextStagePub, err := g.middlewareHandler.CreateDirectExchangeStandalone(g.conf.nextStagePub)
	if err != nil {
		return fmt.Errorf("error creating exchange handler for transactions.items: %v", err)
	}

	g.log.Infof("Setting up results request Exchange for group %s", g.conf.id)
	broadcastResultsRequestExchangeName := fmt.Sprintf("group.%s.results.request", g.conf.ofType)
	broadcastResultsRequestPub, err := g.middlewareHandler.CreateFanoutExchangeStandalone(broadcastResultsRequestExchangeName)
	if err != nil {
		return fmt.Errorf("error creating exchange handler for %s: %v", broadcastResultsRequestExchangeName, err)
	}

	g.log.Infof("Setting up results request SUB for group %s", g.conf.id)
	broadcastResultsRequestSubQueueName := fmt.Sprintf("group.%s.results.request.%s", g.conf.ofType, g.conf.id)
	broadcastResultsRequestSub, err := g.middlewareHandler.CreateQueue(broadcastResultsRequestSubQueueName)
	if err != nil {
		return fmt.Errorf("error creating results request queue for %s: %v", broadcastResultsRequestSubQueueName, err)
	}
	err = g.middlewareHandler.BindQueue(broadcastResultsRequestSubQueueName, broadcastResultsRequestExchangeName, "")

	if err != nil {
		return fmt.Errorf("error preparing results request queue for %s: %v", g.conf.ofType, err)
	}

	g.log.Info("Exchange handlers successfully created")
	g.exchangeHandlers = MiddlewareHandlers{
		prevStageSub:               *prevStageSub,
		nextStagePub:               *nextStagePub,
		privateQueueSub:            *privateQueueSub,
		privateQueuesPub:           privateQueuesPub,
		broadcastResultsRequestPub: *broadcastResultsRequestPub,
		broadcastResultsRequestSub: *broadcastResultsRequestSub,
	}

	return nil
}

func (g *GroupByGenericWorker) SendToQueue(queueName string, message []byte) middleware.MessageMiddlewareError {
	// declare queue many to one (many publishers one consumer)
	g.middlewareMutex.Lock()
	defer g.middlewareMutex.Unlock()
	queue, err := g.middlewareHandler.CreateQueue(queueName)

	if err != nil {
		g.log.Errorf("Failed to declare queue %s: %v", queueName, err)
		return middleware.MessageMiddlewareMessageError
	}
	sendErr := queue.Send(message)
	if sendErr != middleware.MessageMiddlewareSuccess {
		g.log.Errorf("Failed to send message to queue %s: %v", queueName, sendErr)
		return middleware.MessageMiddlewareMessageError
	}
	return middleware.MessageMiddlewareSuccess
}

func (g *GroupByGenericWorker) sendBytesNextStage(msgBytes []byte) error {
	g.middlewareMutex.Lock()
	sendErr := g.exchangeHandlers.nextStagePub.Send(msgBytes)
	g.middlewareMutex.Unlock()
	if sendErr != 0 {
		return fmt.Errorf("error sending message to next stage: %d", sendErr)
	}
	return nil
}

func (g *GroupByGenericWorker) dispatchMessage(message amqp.Delivery) error {
	message_id, destination_id := middleware.GetMessageId(message.Body, g.conf.count)
	err := g.exchangeHandlers.privateQueuesPub[destination_id].SendWithId(message.Body, message_id)
	if err != middleware.MessageMiddlewareSuccess {
		answerMessage(NACK_REQUEUE, message)
		return fmt.Errorf("failed to dispatch message to private queue %d: %v", destination_id, err)
	}
	// DISCLAMER: This is just for simulation purposes
	// if rand.Float64() < REQUEUE_PROBABILITY {
	// 	answerMessage(NACK_REQUEUE, message)
	// 	g.log.Warningf("Simulating message requeue for message %s", message_id)
	// 	return fmt.Errorf("simulated message requeue for message %s", message_id)
	// }
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
