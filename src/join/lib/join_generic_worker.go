package join

import (
	"common/logger"
	"common/middleware"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ClientId = string

type JoinGenericWorker struct {
	log               *logging.Logger
	middlewareHandler *middleware.MiddlewareHandler
	sigChan           chan os.Signal
	isRunning         bool

	conf               JoinWorkerConfig
	middlewareHandlers JoinMiddlewareHandlers
	errChan            chan middleware.MessageMiddlewareError

	mutex        sync.Mutex
	clientsStats map[string]*middleware.ClientStats

	sideTable         map[ClientId][]string
	sideTableReceived chan int

	gatherResultsChans map[ClientId]chan int // to signal when a result has been gathered
}

type JoinMiddlewareHandlers struct {
	prevStageSub               middleware.MessageMiddlewareQueue
	sideTableSub               middleware.MessageMiddlewareQueue
	nextStagePubs              map[string]middleware.MessageMiddlewareExchange
	broadcastCountPub          middleware.MessageMiddlewareExchange
	broadcastCountSub          middleware.MessageMiddlewareQueue
	broadcastResultsRequestPub middleware.MessageMiddlewareExchange
	broadcastResultsRequestSub middleware.MessageMiddlewareQueue
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

		mutex:        sync.Mutex{},
		clientsStats: make(map[string]*middleware.ClientStats),

		sideTable:         map[string][]string{},
		sideTableReceived: make(chan int, SINGLE_ITEM_BUFFER_LEN),

		gatherResultsChans: make(map[ClientId]chan int),
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
		nextStagePubs:              make(map[string]middleware.MessageMiddlewareExchange),
		broadcastCountPub:          *broadcastCountPub,
		broadcastCountSub:          *broadcastCountSub,
		broadcastResultsRequestPub: *broadcastResultsRequestPub,
		broadcastResultsRequestSub: *broadcastResultsRequestSub,
	}
	return nil
}

func (j *JoinGenericWorker) gatherAndMergePartialResults(message amqp.Delivery) error {

	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		j.log.Errorf("Failed to parse message: %v", err)
		answerMessage(NACK_DISCARD, message)
		return err
	}
	j.log.Infof("Gathering partial results for client %s and dataType %s", msg.ClientId, msg.DataType)

	otherResults := msg.Payload

	j.mutex.Lock()
	_, exists := j.sideTable[msg.ClientId]
	if !exists {
		j.sideTable[msg.ClientId] = []string{}
	}

	j.sideTable[msg.ClientId] = append(j.parseOnlyAlreadyJoinedLines(j.sideTable[msg.ClientId]), otherResults...)
	j.mutex.Unlock()

	j.log.Infof("Partial results merged for client %s and dataType %s", msg.ClientId, msg.DataType)
	// Signal that a result has been gathered
	if ch, exists := j.gatherResultsChans[msg.ClientId]; exists {
		ch <- 1
	}
	answerMessage(ACK, message)
	return nil
}

func (j *JoinGenericWorker) gatherOtherPartialResults(eofMessage amqp.Delivery, eofMsg middleware.Message) {
	countToWaitResults := j.conf.count - 1
	if countToWaitResults <= 0 {
		j.log.Infof("No need to gather other partial results, only one instance for client %s and dataType %s", "", "")
		return
	}

	// Create Ephemeral queue
	queueName := fmt.Sprintf("join.%s.results.request.gather.%s", j.conf.ofType, eofMsg.ClientId)
	queue, err := j.middlewareHandler.CreateQueue(queueName)
	if err != nil {
		j.log.Errorf("Failed to declare ephemeral queue: %v", err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}
	j.log.Infof("Requesting results to receive in queue %s for client %s and dataType %s", queueName, eofMsg.ClientId, eofMsg.DataType)
	requestMsg := middleware.NewMessageResultsRequest(j.conf.id, queueName, eofMsg.ClientId, eofMsg.DataType)
	requestBytes, err := requestMsg.ToBytes()
	if err != nil {
		j.log.Errorf("Failed to serialize message: %v", err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}

	// Broadcast results request
	j.middlewareHandlers.broadcastResultsRequestPub.Send(requestBytes)

	// Create channel to gather results
	j.gatherResultsChans[eofMsg.ClientId] = make(chan int, countToWaitResults)
	// Consume from ephemeral queue
	j.log.Infof("Consuming results from queue %s for client %s and dataType %s", queueName, eofMsg.ClientId, eofMsg.DataType)
	queue.StartConsuming(j.gatherAndMergePartialResults, j.errChan)

	for i := range countToWaitResults {
		j.log.Infof("Waiting for partial results %d/%d clients for client %s and dataType %s", i+1, countToWaitResults, eofMsg.ClientId, eofMsg.DataType)
		<-j.gatherResultsChans[eofMsg.ClientId]
		j.log.Infof("Received partial results %d/%d clients for client %s and dataType %s", i+1, countToWaitResults, eofMsg.ClientId, eofMsg.DataType)
	}
	// Stop consuming and delete ephemeral queue
	queue.StopConsuming()
	queue.Delete()
	j.log.Infof("All partial results received for client %s and dataType %s", eofMsg.ClientId, eofMsg.DataType)
	delete(j.gatherResultsChans, eofMsg.ClientId)
}

func (j *JoinGenericWorker) gatherResultsAndSendEof(eofMessage amqp.Delivery, eofMsg middleware.Message, clientStats *middleware.ClientStats) {
	j.gatherOtherPartialResults(eofMessage, eofMsg)

	// SEND RESULTS
	j.mutex.Lock()
	currentResults, exists := j.sideTable[eofMsg.ClientId]
	if !exists {
		j.sideTable[eofMsg.ClientId] = []string{}
	}
	j.mutex.Unlock()

	response := middleware.NewMessage(eofMsg.DataType, eofMsg.ClientId, currentResults, false, eofMsg.QueryId)

	destinationRouteKey, middleError := j.sendNextStage(*response)
	if middleError != nil {
		j.log.Errorf("problem while sending message to %s: %v", destinationRouteKey, middleError)
		return
	}
	j.log.Infof("Sent consolidated results: %s", destinationRouteKey)
	emitted := 1

	// update emitted count and send eof
	eofMsg.TotalEmitted = emitted
	_, err := j.sendNextStage(eofMsg)
	if err != nil {
		j.log.Errorf("Failed to send EOF message to next stage: %v", err)
		answerMessage(NACK_DISCARD, eofMessage)
		return
	}

	answerMessage(ACK, eofMessage)
	j.log.Infof("Sent EOF message to next stage for client %s and dataType %s. Emitted count: %d", eofMsg.ClientId, eofMsg.DataType, eofMsg.TotalEmitted)

	// DELETE AFTER SENDING
	j.mutex.Lock()
	delete(j.sideTable, eofMsg.ClientId)
	j.mutex.Unlock()
}

func (j *JoinGenericWorker) handleEofMessageForJoinerUsers(eofMessage amqp.Delivery, eofMsg middleware.Message) {
	j.mutex.Lock()
	clientStats := j.getClientStats(eofMsg.ClientId)
	j.mutex.Unlock()

	j.log.Infof("Received EOF message for client %s and dataType %s. Expecting %d processed messages", eofMsg.ClientId, eofMsg.DataType, eofMsg.TotalEmitted)

	if clientStats.GetProcessed(eofMsg.DataType) < eofMsg.TotalEmitted {
		j.log.Infof("Not all messages processed yet for client %s and dataType %s", eofMsg.ClientId, eofMsg.DataType)
		j.log.Infof("Waiting for all messages to be processed for client %s and dataType %s", eofMsg.ClientId, eofMsg.DataType)
		clientStats.WaitForEofChan(eofMsg.DataType)
	}

	j.log.Infof("Initiating gathering results and sending EOF for client %s and dataType %s", eofMsg.ClientId, eofMsg.DataType)
	j.gatherResultsAndSendEof(eofMessage, eofMsg, clientStats)
}

func (j *JoinGenericWorker) joinWithPayload(message amqp.Delivery) error {
	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	j.mutex.Lock()
	_, exists := j.sideTable[msg.ClientId]
	if !exists {
		j.sideTable[msg.ClientId] = []string{}
	}
	j.mutex.Unlock()

	// Given this method is used only on the Users Joiner
	msg.QueryId = 4

	if msg.IsEof {
		j.mutex.Lock()
		clientStats := j.getClientStats(msg.ClientId)
		clientStats.SetEof(msg.DataType, msg.TotalEmitted)
		j.mutex.Unlock()
		go j.handleEofMessageForJoinerUsers(message, *msg)
		return nil
	}

	j.log.Debugf("Received payload: %v", msg.Payload)
	j.mutex.Lock()
	j.sideTable[msg.ClientId] = j.conf.messageCallbackUpdateSideTable(j.sideTable[msg.ClientId], msg.Payload)
	j.mutex.Unlock()

	answerMessage(ACK, message)
	j.log.Debug("Partially updated side table")

	msgProcessed := middleware.NewMessageProcessed(msg.DataType, msg.ClientId, true, msg.QueryId)
	err = j.sendProcessedMessage(msgProcessed)
	if err != nil {
		j.log.Errorf("Failed to send processed count message: %v", err)
		answerMessage(NACK_REQUEUE, message)
		return err
	}

	return nil
}

func (j *JoinGenericWorker) joinWithSideTable(message amqp.Delivery) error {
	msg, err := middleware.NewMessageGroupedFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	j.mutex.Lock()
	_, exists := j.sideTable[msg.ClientId]
	if !exists {
		j.sideTable[msg.ClientId] = []string{}
	}
	j.mutex.Unlock()

	switch j.conf.ofType {
	case JOIN_ITEMS_TYPE:
		msg.QueryId = 2
	case JOIN_STORE_Q3_TYPE:
		msg.QueryId = 3
	case JOIN_USERS_TYPE:
		msg.QueryId = 4
	}

	if msg.IsEof {
		j.mutex.Lock()
		clientStats := j.getClientStats(msg.ClientId)
		clientStats.SetEof(msg.DataType, msg.TotalEmitted)
		j.mutex.Unlock()
		go j.handleEofMessage(message, *msg.ToMessage())
		return nil
	}

	j.log.Debugf("Received payload: %v", msg.Payload)
	joined := j.conf.messageCallback(NewJoiner(), j.sideTable[msg.ClientId], msg.Payload)
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
	j.mutex.Lock()
	clientStats := j.getClientStats(eofMsg.ClientId)
	j.mutex.Unlock()

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

func (j *JoinGenericWorker) saveSideTable(message amqp.Delivery) error {

	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	j.mutex.Lock()
	_, exists := j.sideTable[msg.ClientId]
	if !exists {
		j.sideTable[msg.ClientId] = []string{}
	}
	j.mutex.Unlock()

	if msg.IsEof {
		j.log.Infof("Received EOF for %s. Ready to Join.", j.conf.ofType)
		answerMessage(ACK, message)
		j.sideTableReceived <- ACTIVITY
		return nil
	}

	j.mutex.Lock()
	j.sideTable[msg.ClientId] = append(j.sideTable[msg.ClientId], msg.Payload...)
	j.mutex.Unlock()

	j.log.Infof("Side table size: %d", len(j.sideTable[msg.ClientId]))
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

	j.mutex.Lock()
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

	j.mutex.Unlock()

	answerMessage(ACK, message)
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

func (j *JoinGenericWorker) parseOnlyAlreadyJoinedLines(lines []string) []string {
	result := []string{}

	for _, line := range lines {
		splitted := strings.SplitN(line, ",", 2)
		if strings.Contains(splitted[1], "-") {
			result = append(result, line)
		}
	}

	return result
}

func (j *JoinGenericWorker) gatherAndSendPartialResults(message amqp.Delivery) error {
	msg, err := middleware.NewMessageResultsRequestFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}
	j.log.Infof("Received request to gather and send partial results from %s to queue %s", msg.Origin, msg.QueueName)
	if msg.Origin == j.conf.id {
		j.log.Infof("Ignoring request to gather and send partial results from myself %s", msg.Origin)
		answerMessage(ACK, message)
		return nil
	}

	j.mutex.Lock()
	_, exists := j.sideTable[msg.ClientId]
	if !exists {
		j.sideTable[msg.ClientId] = []string{}
	}
	partialResults := j.parseOnlyAlreadyJoinedLines(j.sideTable[msg.ClientId])
	j.mutex.Unlock()

	messageToSend := middleware.NewMessage(msg.DataType, msg.ClientId, partialResults, false, 0)
	responseBytes, err := messageToSend.ToBytes()
	if err != nil {
		j.log.Errorf("%v", err)
		answerMessage(NACK_DISCARD, message)
		return err
	}
	j.log.Infof("Sending partial results to %s", msg.QueueName)

	// SEND TO REQUESTOR
	middleError := j.SendToQueue(msg.QueueName, responseBytes)
	if middleError != middleware.MessageMiddlewareSuccess {
		answerMessage(NACK_REQUEUE, message)
		return fmt.Errorf("problem while sending message to %s", msg.QueueName)
	}

	j.log.Infof("Partial results successfully sent to %s", msg.QueueName)
	// DELETE AFTER SENDING
	// At this point, client has finished, results have been sent to the requester, so we can delete the stored group
	j.mutex.Lock()
	delete(j.sideTable, msg.ClientId)
	j.mutex.Unlock()
	j.log.Infof("Deleted stored sideTable for client %s", msg.ClientId)

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
	j.middlewareHandlers.sideTableSub.StartConsuming(j.saveSideTable, j.errChan)
	<-j.sideTableReceived

	if !j.isRunning {
		return nil
	}

	j.middlewareHandlers.broadcastCountSub.StartConsuming(j.processedCountMessage, j.errChan)
	if j.conf.ofType == JOIN_USERS_TYPE {
		j.middlewareHandlers.prevStageSub.StartConsuming(j.joinWithPayload, j.errChan)
		j.middlewareHandlers.broadcastResultsRequestSub.StartConsuming(j.gatherAndSendPartialResults, j.errChan)
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
