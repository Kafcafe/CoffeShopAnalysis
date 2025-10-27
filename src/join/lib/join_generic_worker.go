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
	"time"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ClientId = string
type DataType = string

type JoinGenericWorker struct {
	log               *logging.Logger
	middlewareHandler *middleware.MiddlewareHandler
	sigChan           chan os.Signal
	isRunning         bool

	conf               JoinWorkerConfig
	middlewareHandlers JoinMiddlewareHandlers
	errChan            chan middleware.MessageMiddlewareError

	mutex           sync.Mutex
	middlewareMutex sync.Mutex
	clientsStats    map[string]*middleware.ClientStats

	sideTable         map[ClientId][]string
	mainTable         map[ClientId][]string
	sideTableReceived map[ClientId]chan int

	resultsChans map[ClientId]map[DataType]chan middleware.MessageResultsResponse
}

type JoinMiddlewareHandlers struct {
	prevStageSub               middleware.MessageMiddlewareQueue
	sideTableSub               middleware.MessageMiddlewareQueue
	nextStagePubs              map[string]middleware.MessageMiddlewareExchange
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

// handleSignal listens for SIGTERM signal and triggers shutdown.
func (j *JoinGenericWorker) handleSignal() {
	<-j.sigChan
	j.log.Info("Handling signal")
	j.Shutdown()
}

func NewJoinWorker(rabbitConf middleware.RabbitConfig, config JoinWorkerConfig) (*JoinGenericWorker, error) {
	log := logger.GetLoggerWithPrefix("[JOINER-" + config.id + "]")

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

		mutex:           sync.Mutex{},
		middlewareMutex: sync.Mutex{},
		clientsStats:    make(map[string]*middleware.ClientStats),

		sideTable:         map[string][]string{},
		mainTable:         map[string][]string{},
		sideTableReceived: make(map[ClientId]chan int),

		resultsChans: make(map[ClientId]map[DataType]chan middleware.MessageResultsResponse),
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
		broadcastResultsRequestPub: *broadcastResultsRequestPub,
		broadcastResultsRequestSub: *broadcastResultsRequestSub,
	}
	return nil
}

func (j *JoinGenericWorker) broadcastAndWaitForResults(requestBytes []byte, clientId ClientId, dataType DataType, expectedEmitted int) (processed, emitted int, results []string, timeout bool) {
	for retriesCount := 0; retriesCount < middleware.MAX_EOF_RETRIES; retriesCount++ {
		processed = 0
		emitted = 0
		results = []string{}
		timeout = false
		timeoutDuration := time.Second * time.Duration(middleware.RESPONSE_TIMEOUT_SEC*(retriesCount+1))
		if sendErr := j.middlewareHandlers.broadcastResultsRequestPub.Send(requestBytes); sendErr != middleware.MessageMiddlewareSuccess {
			j.log.Errorf("Failed to send results request message to broadcast exchange: %v", sendErr)
			break
		}
		j.log.Infof("Sent results request message to broadcast exchange for client %s and dataType %s. Attempt %d/%d", clientId, dataType, retriesCount+1, middleware.MAX_EOF_RETRIES)
		for !timeout && processed < expectedEmitted {
			select {
			case msg := <-j.resultsChans[clientId][dataType]:
				processed += msg.Processed
				emitted += msg.Emitted
				if msg.Payload != nil {
					results = append(results, msg.Payload...)
				}
			case <-time.After(timeoutDuration):
				j.log.Warningf("Timeout waiting for results response for client %s and datatype %s after %d seconds. Processed %d/%d", clientId, dataType, middleware.RESPONSE_TIMEOUT_SEC, processed, expectedEmitted)
				timeout = true
			}
		}
		if !timeout {
			break
		}
	}
	return processed, emitted, results, timeout
}

func (j *JoinGenericWorker) joinWithPayload(message amqp.Delivery) error {
	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	j.log.Debugf("Received message for client %s and datatype %s", msg.ClientId, msg.DataType)

	j.mutex.Lock()
	_, exists := j.sideTable[msg.ClientId]
	if _, ok := j.sideTableReceived[msg.ClientId]; !ok {
		j.sideTableReceived[msg.ClientId] = make(chan int, SINGLE_ITEM_BUFFER_LEN)
	}
	j.mutex.Unlock()
	if !exists {
		j.log.Warning("Side table not ready!!!! Waiting...")
		<-j.sideTableReceived[msg.ClientId]
		j.log.Warning("Side table ready!!! Continuing...")
	}

	msg.QueryId = j.conf.queryId

	if msg.IsEof {
		go j.handleEofMessage(message, *msg)
		return nil
	}

	j.mutex.Lock()
	partialUpdate := j.conf.messageCallbackUpdateSideTable(j.sideTable[msg.ClientId], msg.Payload)
	j.sideTable[msg.ClientId] = partialUpdate
	j.getClientStats(msg.ClientId).Add(msg.DataType, true, false)
	j.mutex.Unlock()

	answerMessage(ACK, message)

	return nil
}

func (j *JoinGenericWorker) joinWithSideTable(message amqp.Delivery) error {
	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}
	msg.QueryId = j.conf.queryId

	if msg.IsEof {
		go j.handleEofMessage(message, *msg)
		return nil
	}

	flattenedPayload := j.conf.flattenPayload(msg.GroupedPayload)
	j.mutex.Lock()
	j.mainTable[msg.ClientId] = append(j.mainTable[msg.ClientId], flattenedPayload...)
	j.getClientStats(msg.ClientId).Add(msg.DataType, true, false)
	j.mutex.Unlock()

	answerMessage(ACK, message)
	return nil
}

func (j *JoinGenericWorker) ensureResultsChanExists(clientId ClientId, dataType DataType) {
	if _, exists := j.resultsChans[clientId]; !exists {
		j.resultsChans[clientId] = make(map[DataType]chan middleware.MessageResultsResponse)
	}
	if _, exists := j.resultsChans[clientId][dataType]; !exists {
		j.resultsChans[clientId][dataType] = make(chan middleware.MessageResultsResponse)
	}
}

func (j *JoinGenericWorker) processResultsResponse(message amqp.Delivery) error {
	msg, err := middleware.NewMessageResultsResponseFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	j.log.Infof("Received results response from %s for client %s and datatype %s: processed=%d, emitted=%d", msg.Origin, msg.ClientId, msg.DataType, msg.Processed, msg.Emitted)
	j.resultsChans[msg.ClientId][msg.DataType] <- *msg
	answerMessage(ACK, message)
	return nil
}

func (j *JoinGenericWorker) handleEofMessage(eofMessage amqp.Delivery, eofMsg middleware.Message) {
	j.log.Infof("Received EOF message for client %s and dataType %s. Expecting %d processed messages", eofMsg.ClientId, eofMsg.DataType, eofMsg.TotalEmitted)
	mh, err := middleware.NewMiddlewareHandler(j.middlewareHandler.RabbitConn)
	if err != nil {
		j.log.Errorf("Failed to create middleware handler: %v", err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}
	queueName := fmt.Sprintf("join.%s.results.request.gather.%s", j.conf.ofType, eofMsg.ClientId)
	queue, err := mh.CreateQueue(queueName)
	if err != nil {
		j.log.Errorf("Failed to create ephemeral queue %s: %v", queueName, err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}

	requestMsg := middleware.NewGatherResultsRequest(j.conf.id, queueName, eofMsg.ClientId, eofMsg.DataType)
	requestBytes, err := requestMsg.ToBytes()
	if err != nil {
		j.log.Errorf("Failed to serialize results request message: %v", err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}

	j.ensureResultsChanExists(eofMsg.ClientId, eofMsg.DataType)
	queue.StartConsuming(j.processResultsResponse, j.errChan)

	processed, emitted, results, timeout := j.broadcastAndWaitForResults(requestBytes, eofMsg.ClientId, eofMsg.DataType, eofMsg.TotalEmitted)
	if processed == 0 {
		j.log.Errorf("Unexpected error waiting results for client %s and dataType %s", eofMsg.ClientId, eofMsg.DataType)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}
	if timeout {
		j.log.Warningf("Could not gather all results for client %s and dataType %s after %d retries. Proceeding with partial results: processed %d/%d, emitted %d", eofMsg.ClientId, eofMsg.DataType, middleware.MAX_EOF_RETRIES, processed, eofMsg.TotalEmitted, emitted)
	}
	delete(j.resultsChans[eofMsg.ClientId], eofMsg.DataType)
	delete(j.resultsChans, eofMsg.ClientId)
	if err := queue.StopConsuming(); err != middleware.MessageMiddlewareSuccess {
		j.log.Warningf("Failed to stop consuming ephemeral queue %s: %v", queueName, err)
	}
	if err := queue.Delete(); err != middleware.MessageMiddlewareSuccess {
		j.log.Warningf("Failed to delete ephemeral queue %s: %v", queueName, err)
	}

	response := middleware.NewMessageWithPayload(eofMsg.DataType, eofMsg.ClientId, results, false, eofMsg.QueryId)

	if middleError := j.sendNextStage(*response); middleError != nil {
		j.log.Errorf("problem while sending message to %s: %v", j.conf.nextStagePubs[j.conf.ofType], middleError)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}
	emitted++

	expectedTotal := eofMsg.TotalEmitted
	eofMsg.TotalEmitted = emitted
	if err := j.sendNextStage(eofMsg); err != nil {
		j.log.Errorf("Failed to send EOF message to next stage: %v. Requeuing message...", err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}

	j.log.Infof("Sent EOF message to next stage for client %s and dataType %s. Processed count: %d/%d, Emitted count: %d", eofMsg.ClientId, eofMsg.DataType, processed, expectedTotal, emitted)
	answerMessage(ACK, eofMessage)

	clearMsg := middleware.NewClearResultsRequest(j.conf.id, "", eofMsg.ClientId, eofMsg.DataType)
	clearMsgBytes, err := clearMsg.ToBytes()
	if err != nil {
		j.log.Warningf("Failed to serialize results request message: %v", err)
	}
	if sendErr := j.middlewareHandlers.broadcastResultsRequestPub.Send(clearMsgBytes); sendErr != middleware.MessageMiddlewareSuccess {
		j.log.Warningf("Failed to send results request message to broadcast exchange: %v", sendErr)
	}
}

func (j *JoinGenericWorker) sendResultsRequest(message amqp.Delivery) error {
	msg, err := middleware.NewMessageResultsRequestFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	if msg.RequestType == middleware.RESULTS_REQUEST_TYPE_CLEAR {
		j.getClientStats(msg.ClientId).Clear(msg.DataType)
		delete(j.sideTable, msg.ClientId)
		delete(j.mainTable, msg.ClientId)
		delete(j.sideTableReceived, msg.ClientId)
		j.log.Infof("Cleared stats for client %s and datatype %s", msg.ClientId, msg.DataType)
		answerMessage(ACK, message)
		return nil
	}

	j.log.Infof("Received results request message from %s for client %s and datatype %s", msg.Origin, msg.ClientId, msg.DataType)
	processed, emitted := j.getClientStats(msg.ClientId).GetStats(msg.DataType)

	responseMsg := middleware.MessageResultsResponse{
		Origin:    j.conf.id,
		ClientId:  msg.ClientId,
		DataType:  msg.DataType,
		Processed: processed,
		Emitted:   emitted,
	}

	if j.conf.ofType == JOIN_USERS_TYPE {
		j.mutex.Lock()
		currentResults, exists := j.sideTable[msg.ClientId]
		j.mutex.Unlock()
		if !exists {
			currentResults = []string{}
		}
		responseMsg.Payload = j.parseOnlyAlreadyJoinedLines(currentResults)
	} else {
		j.mutex.Lock()
		currentResults, exists := j.mainTable[msg.ClientId]
		sideTable := j.sideTable[msg.ClientId]
		j.mutex.Unlock()
		if !exists {
			currentResults = []string{}
		}
		responseMsg.Payload = j.conf.joinTables(NewJoiner(), sideTable, currentResults)
	}

	responseBytes, err := responseMsg.ToBytes()
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	if sendErr := j.SendToQueue(msg.QueueName, responseBytes); sendErr != middleware.MessageMiddlewareSuccess {
		answerMessage(NACK_REQUEUE, message)
		return fmt.Errorf("failed to send results response to queue %s", msg.QueueName)
	}
	j.log.Infof("Sent results response to %s for client %s and datatype %s: processed=%d, emitted=%d", msg.QueueName, msg.ClientId, msg.DataType, processed, emitted)
	answerMessage(ACK, message)
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

func (j *JoinGenericWorker) saveSideTable(message amqp.Delivery) error {

	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	if msg.IsEof {
		j.log.Infof("Received EOF for %s. Ready to Join.", j.conf.ofType)
		answerMessage(ACK, message)
		j.mutex.Lock()
		if _, exists := j.sideTableReceived[msg.ClientId]; !exists {
			j.sideTableReceived[msg.ClientId] = make(chan int, SINGLE_ITEM_BUFFER_LEN)
		}
		j.mutex.Unlock()
		j.sideTableReceived[msg.ClientId] <- ACTIVITY
		return nil
	}

	j.mutex.Lock()
	_, exists := j.sideTable[msg.ClientId]
	if !exists {
		j.sideTable[msg.ClientId] = []string{}
	}
	j.sideTable[msg.ClientId] = append(j.sideTable[msg.ClientId], msg.Payload...)
	j.mutex.Unlock()

	j.log.Infof("Side table size for client %s: %d", msg.ClientId, len(j.sideTable[msg.ClientId]))
	answerMessage(ACK, message)
	return nil
}

func (j *JoinGenericWorker) getClientStats(clientId string) *middleware.ClientStats {
	if _, exists := j.clientsStats[clientId]; !exists {
		j.clientsStats[clientId] = middleware.NewClientStats()
	}
	return j.clientsStats[clientId]
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

func (j *JoinGenericWorker) Run() error {
	go j.handleSignal()

	err := j.createExchangeHandlers()
	if err != nil {
		return fmt.Errorf("failed to create exchange handlers: %v", err)
	}

	j.log.Info("Waiting to receive side table...")
	j.middlewareHandlers.sideTableSub.StartConsuming(j.saveSideTable, j.errChan)

	if !j.isRunning {
		return nil
	}

	if j.conf.ofType == JOIN_USERS_TYPE {
		j.middlewareHandlers.prevStageSub.StartConsuming(j.joinWithPayload, j.errChan)
	} else {
		j.middlewareHandlers.prevStageSub.StartConsuming(j.joinWithSideTable, j.errChan)
	}
	j.middlewareHandlers.broadcastResultsRequestSub.StartConsuming(j.sendResultsRequest, j.errChan)

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
	j.errChan <- middleware.MessageMiddlewareSuccess

	j.middlewareHandlers.Shutdown()
	j.middlewareHandler.Close()

	j.log.Info("Shutdown complete")
}
