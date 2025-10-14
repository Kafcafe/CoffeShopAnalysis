package join

import (
	"common/logger"
	"common/middleware"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type JoinGenericWorker struct {
	log               *logging.Logger
	middlewareHandler *middleware.MiddlewareHandler
	sigChan           chan os.Signal
	isRunning         bool

	conf               JoinWorkerConfig
	middlewareHandlers JoinMiddlewareHandlers
	errChan            chan middleware.MessageMiddlewareError

	clientsStats map[string]*middleware.ClientStats

	sideTable         []string
	sideTableReceived chan int
}

type JoinMiddlewareHandlers struct {
	prevStageSub      middleware.MessageMiddlewareQueue
	sideTableSub      middleware.MessageMiddlewareQueue
	nextStagePubs     map[string]middleware.MessageMiddlewareExchange
	broadcastCountPub middleware.MessageMiddlewareExchange
	broadcastCountSub middleware.MessageMiddlewareQueue
}

func (mh *JoinMiddlewareHandlers) Shutdown() {
	mh.prevStageSub.Close()
	mh.sideTableSub.Close()
	for _, nextStagePub := range mh.nextStagePubs {
		nextStagePub.Close()
	}
	mh.broadcastCountPub.Close()
	mh.broadcastCountSub.Close()
}

// handleSignal listens for SIGTERM signal and triggers shutdown.
func (j *JoinGenericWorker) handleSignal() {
	<-j.sigChan
	j.log.Info("Handling signal")
	j.Shutdown()
}

func NewJoinWorker(rabbitConf middleware.RabbitConfig, config JoinWorkerConfig) (*JoinGenericWorker, error) {
	log := logger.GetLoggerWithPrefix("[JOINER-" + config.id + "] ")

	log.Infof("Establishing connection with RabbitMQ on address %s:%d", rabbitConf.Host, rabbitConf.Port)

	rabbitConn, err := middleware.NewRabbitConnection(&rabbitConf)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	middlewareHandler, err := middleware.NewMiddlewareHandler(rabbitConn)
	if err != nil {
		return nil, fmt.Errorf("failed to create middleware handler: %v", err)
	}

	log.Info("Connection with RabbitMQ successfully established")

	sigChan := make(chan os.Signal, SINGLE_ITEM_BUFFER_LEN)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	return &JoinGenericWorker{
		log:               log,
		middlewareHandler: middlewareHandler,
		sigChan:           sigChan,
		isRunning:         true,

		conf:    config,
		errChan: make(chan middleware.MessageMiddlewareError, ERROR_CHANNEL_BUFFER_SIZE),

		clientsStats: make(map[string]*middleware.ClientStats),

		sideTable:         make([]string, 0),
		sideTableReceived: make(chan int, SINGLE_ITEM_BUFFER_LEN),
	}, nil
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

	// NEXT STAGE PUB
	nextStagePubs := make(map[string]middleware.MessageMiddlewareExchange)
	nextStagePub, err := j.middlewareHandler.CreateDirectExchangeStandalone(j.conf.nextStagePubs[j.conf.ofType])
	if err != nil {
		return fmt.Errorf("error creating exchange handler for results.q2: %v", err)
	}
	nextStagePubs[j.conf.ofType] = *nextStagePub

	// BROADCAST COUNT PUB/SUB
	j.log.Infof("Setting up count PUB for join %s", j.conf.id)
	broadcastCountPubRoutKey := fmt.Sprintf("join.%s.count", j.conf.ofType)
	broadcastCountPub, err := j.middlewareHandler.CreateFanoutExchangeStandalone(broadcastCountPubRoutKey)
	if err != nil {
		return fmt.Errorf("error creating exchange handler for %s: %v", broadcastCountPubRoutKey, err)
	}

	j.log.Infof("Setting up count SUB for join %s", j.conf.id)
	broadcastCountSubQueueName := fmt.Sprintf("join.%s.count.%s", j.conf.ofType, j.conf.id)
	broadcastCountSub, err := j.middlewareHandler.CreateQueue(broadcastCountSubQueueName)
	if err != nil {
		return fmt.Errorf("error creating count queue for %s: %v", broadcastCountSubQueueName, err)
	}
	err = j.middlewareHandler.BindQueue(broadcastCountSubQueueName, broadcastCountPubRoutKey, "")

	if err != nil {
		return fmt.Errorf("error preparing count queue for %s: %v", j.conf.ofType, err)
	}

	j.middlewareHandlers = JoinMiddlewareHandlers{
		prevStageSub:      *prevStageSub,
		sideTableSub:      *sideTableSub,
		nextStagePubs:     nextStagePubs,
		broadcastCountPub: *broadcastCountPub,
		broadcastCountSub: *broadcastCountSub,
	}
	return nil
}

func (j *JoinGenericWorker) createExchangeHandlersForFinalStage() error {
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

	// NEXT STAGE PUB

	// BROADCAST COUNT PUB/SUB
	j.log.Infof("Setting up count PUB for join %s", j.conf.id)
	broadcastCountPubRoutKey := fmt.Sprintf("join.%s.count", j.conf.ofType)
	broadcastCountPub, err := j.middlewareHandler.CreateFanoutExchangeStandalone(broadcastCountPubRoutKey)
	if err != nil {
		return fmt.Errorf("error creating exchange handler for %s: %v", broadcastCountPubRoutKey, err)
	}

	j.log.Infof("Setting up count SUB for join %s", j.conf.id)
	broadcastCountSubQueueName := fmt.Sprintf("join.%s.count.%s", j.conf.ofType, j.conf.id)
	broadcastCountSub, err := j.middlewareHandler.CreateQueue(broadcastCountSubQueueName)
	if err != nil {
		return fmt.Errorf("error creating count queue for %s: %v", broadcastCountSubQueueName, err)
	}
	err = j.middlewareHandler.BindQueue(broadcastCountSubQueueName, broadcastCountPubRoutKey, "")

	if err != nil {
		return fmt.Errorf("error preparing count queue for %s: %v", j.conf.ofType, err)
	}

	j.middlewareHandlers = JoinMiddlewareHandlers{
		prevStageSub:      *prevStageSub,
		sideTableSub:      *sideTableSub,
		nextStagePubs:     make(map[string]middleware.MessageMiddlewareExchange),
		broadcastCountPub: *broadcastCountPub,
		broadcastCountSub: *broadcastCountSub,
	}
	return nil
}

func (j *JoinGenericWorker) joinWithPayload(message amqp.Delivery) error {
	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	switch j.conf.ofType {
	case JOIN_ITEMS_TYPE:
		msg.QueryId = 2
	case JOIN_STORE_Q3_TYPE:
		msg.QueryId = 3
	case JOIN_USERS_TYPE:
		msg.QueryId = 4
	}

	if !msg.IsEof {
		j.log.Debugf("Received payload: %v", msg.Payload)
		j.sideTable = j.conf.messageCallbackUpdateSideTable(j.sideTable, msg.Payload)
		answerMessage(ACK, message)
		j.log.Debug("Partially updated side table")
		return nil
	}

	j.log.Infof("Received EOF for %s join%s. Sending joined table and EOF", msg.DataType, j.conf.id)

	sideTableMessage := middleware.NewMessage(msg.DataType, msg.ClientId, j.sideTable, false, msg.QueryId)
	destinationRouteKey, err := j.sendNextStage(*sideTableMessage)
	if err != nil {
		answerMessage(NACK_REQUEUE, message)
		return err
	}

	msg.TotalEmitted = 1
	destinationRouteKey, err = j.sendNextStage(*msg)
	if err != nil {
		j.log.Errorf("Failed to send EOF to next stage: %v", err)
		answerMessage(NACK_REQUEUE, message)
		return err
	}
	j.log.Infof("Sent side table and EOF to next stage: %s", destinationRouteKey)
	answerMessage(ACK, message)
	return nil
}

func (j *JoinGenericWorker) joinWithSideTable(message amqp.Delivery) error {
	msg, err := middleware.NewMessageGroupedFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	switch j.conf.ofType {
	case JOIN_ITEMS_TYPE:
		msg.QueryId = 2
	case JOIN_STORE_Q3_TYPE:
		msg.QueryId = 3
	case JOIN_USERS_TYPE:
		msg.QueryId = 4
	}

	if msg.IsEof {
		clientStats := j.getClientStats(msg.ClientId)
		clientStats.SetEof(msg.DataType, msg.TotalEmitted)
		go j.handleEofMessage(message, *msg.ToMessage())
		return nil
	}

	j.log.Debugf("Received payload: %v", msg.Payload)
	joined := j.conf.messageCallback(NewJoiner(), j.sideTable, msg.Payload)
	j.log.Infof("Joined %v items", len(joined))

	msgProcessed := middleware.NewMessageProcessed(msg.DataType, msg.ClientId, true, msg.QueryId)
	err = j.sendProcessedMessage(msgProcessed)
	if err != nil {
		j.log.Errorf("Failed to send processed count message: %v", err)
		answerMessage(NACK_REQUEUE, message)
		return err
	}

	response := middleware.NewMessage(msg.DataType, msg.ClientId, joined, false, msg.QueryId)
	destinationRouteKey, err := j.sendNextStage(*response)
	if err != nil {
		j.log.Errorf("Failed to send joined message to next stage: %v", err)
		answerMessage(NACK_DISCARD, message)
		return err
	}

	answerMessage(ACK, message)
	j.log.Infof("Joined message and sent to next stage: %s", destinationRouteKey)
	return nil
}

func (j *JoinGenericWorker) handleEofMessage(eofMessage amqp.Delivery, eofMsg middleware.Message) {
	clientStats := j.getClientStats(eofMsg.ClientId)

	j.log.Infof("Received EOF message for client %s and dataType %s. Expecting %d processed messages", eofMsg.ClientId, eofMsg.DataType, eofMsg.TotalEmitted)

	if clientStats.GetProcessed(eofMsg.DataType) < eofMsg.TotalEmitted {
		j.log.Infof("Not all messages processed yet for client %s and dataType %s", eofMsg.ClientId, eofMsg.DataType)
		j.log.Infof("Waiting for all messages to be processed for client %s and dataType %s", eofMsg.ClientId, eofMsg.DataType)
		clientStats.WaitForEofChan(eofMsg.DataType)
	}

	j.log.Infof("All messages processed for client %s and dataType %s", eofMsg.ClientId, eofMsg.DataType)

	// update emitted count
	eofMsg.TotalEmitted = clientStats.GetEmitted(eofMsg.DataType)
	destinationRouteKey, err := j.sendNextStage(eofMsg)
	if err != nil {
		j.log.Errorf("Failed to send EOF message to next stage: %v", err)
		answerMessage(NACK_DISCARD, eofMessage)
		return
	}
	answerMessage(ACK, eofMessage)
	j.log.Infof("Sent EOF message to next stage for client %s and dataType %s to %s. Emitted count: %d", eofMsg.ClientId, eofMsg.DataType, destinationRouteKey, eofMsg.TotalEmitted)
}

func (j *JoinGenericWorker) sendProcessedMessage(msgProcessed *middleware.MessageProcessed) error {
	msgProcessedBytes, err := msgProcessed.ToBytes()
	if err != nil {
		return err
	}
	sendErr := j.middlewareHandlers.broadcastCountPub.Send(msgProcessedBytes)
	if sendErr != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to send processed count message: %v", sendErr)
	}
	return nil
}

func (j *JoinGenericWorker) sendNextStage(msgToSend middleware.Message) (nextStagePubRouteKey string, err error) {
	msgBytes, err := msgToSend.ToBytes()
	if err != nil {
		return "", err
	}

	var nextStagePub middleware.MessageMiddlewareExchange
	var exists bool
	var routeKey string

	if j.conf.ofType == JOIN_STORE_TYPE {
		nextStagePub, exists = j.middlewareHandlers.nextStagePubs[j.conf.ofType]
		if !exists {
			return "", fmt.Errorf("received unprocessabble message in sendNextStage of type %s", msgToSend.DataType)
		}
		routeKey = j.conf.nextStagePubs[j.conf.ofType]
	} else {
		nextStagePub, exists = j.middlewareHandlers.nextStagePubs[msgToSend.ClientId]
		if !exists {
			routeKey = fmt.Sprintf("results.%s", msgToSend.ClientId)
			j.log.Infof("Next stage publishing for datatype %s on routeKey %s", msgToSend.DataType, routeKey)
			exchange, err := j.middlewareHandler.CreateDirectExchangeStandalone(routeKey)
			if err != nil {
				return "", fmt.Errorf("error creating exchange handler for %s: %v", routeKey, err)
			}
			j.middlewareHandlers.nextStagePubs[msgToSend.ClientId] = *exchange
			nextStagePub = *exchange
		}
	}

	nextStagePub.Send(msgBytes)
	return routeKey, nil
}

func (j *JoinGenericWorker) storeSideTable(message amqp.Delivery) error {

	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	if msg.IsEof {
		j.log.Infof("Received EOF for %s. Ready to Join.", j.conf.ofType)
		answerMessage(ACK, message)
		j.sideTableReceived <- ACTIVITY
		return nil
	}

	j.sideTable = append(j.sideTable, msg.Payload...)

	j.log.Infof("Side table size: %d", len(j.sideTable))
	answerMessage(ACK, message)
	return nil
}

func (j *JoinGenericWorker) getClientStats(clientId string) *middleware.ClientStats {
	if _, exists := j.clientsStats[clientId]; !exists {
		j.clientsStats[clientId] = middleware.NewClientStats()
	}
	return j.clientsStats[clientId]
}

func (j *JoinGenericWorker) processedCountMessage(message amqp.Delivery) error {
	msg, err := middleware.NewMessageProcessedFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	clientStats := j.getClientStats(msg.ClientId)

	clientStats.AddProcessed(msg.DataType)
	if msg.Emitted {
		clientStats.AddEmitted(msg.DataType)
	}

	if prevEofEmittedCount, exists := clientStats.GetEof(msg.DataType); exists {
		// EOF ARRIVED!
		if clientStats.GetProcessed(msg.DataType) == prevEofEmittedCount {
			j.log.Infof("All messages processed for client %s and dataType %s", msg.ClientId, msg.DataType)
			clientStats.SendEofChan(msg.DataType)
		}
	}

	answerMessage(ACK, message)
	return nil
}
func (j *JoinGenericWorker) Run() error {
	go j.handleSignal()

	if j.conf.ofType == JOIN_STORE_TYPE {
		err := j.createExchangeHandlers()
		if err != nil {
			return fmt.Errorf("failed to create exchange handlers: %v", err)
		}
	} else {
		err := j.createExchangeHandlersForFinalStage()
		if err != nil {
			return fmt.Errorf("failed to create exchange handlers: %v", err)
		}
	}

	j.log.Info("Waiting to receive side table...")
	j.middlewareHandlers.sideTableSub.StartConsuming(j.storeSideTable, j.errChan)
	<-j.sideTableReceived

	if !j.isRunning {
		return nil
	}

	j.middlewareHandlers.broadcastCountSub.StartConsuming(j.processedCountMessage, j.errChan)
	if j.conf.ofType == JOIN_USERS_TYPE {
		// RESTRICTION: Users ONLY can have 1 replica
		j.middlewareHandlers.prevStageSub.StartConsuming(j.joinWithPayload, j.errChan)
	} else {
		j.middlewareHandlers.prevStageSub.StartConsuming(j.joinWithSideTable, j.errChan)
	}

	j.log.Info("Started consuming messages. Ready to join!")
	for err := range j.errChan {
		if err != middleware.MessageMiddlewareSuccess {
			j.log.Errorf("Error found while joining message of type: %v", err)
		}

		if !j.isRunning {
			j.log.Info("Inside error loop: breaking")
			break
		}
	}

	j.log.Info("Finished joining!")
	return nil
}

// Shutdown gracefully stops the acceptor, closing the listener and current client.
func (j *JoinGenericWorker) Shutdown() {
	j.isRunning = false
	j.sideTableReceived <- ACTIVITY
	j.errChan <- middleware.MessageMiddlewareSuccess

	j.middlewareHandlers.Shutdown()
	j.middlewareHandler.Close()

	j.log.Info("Shutdown complete")
}
