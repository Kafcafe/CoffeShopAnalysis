package filters

import (
	"common/logger"
	"common/middleware"
	"common/watch_mesh"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ClientId = string
type DataType = string

type FilterGenericWorker struct {
	log               *logging.Logger
	middlewareHandler *middleware.MiddlewareHandler
	sigChan           chan os.Signal
	isRunning         bool

	filter             Filter
	conf               FilterConfig
	middlewareHandlers MiddlewareHandlers
	errChan            chan middleware.MessageMiddlewareError
	// new eof
	clientsStatsMutex sync.Mutex
	clientsStats      map[ClientId]*middleware.ClientStats
	resultsChans      map[ClientId]map[DataType]chan middleware.MessageResultsResponse
	watchMesh         *watch_mesh.WatchMesh
}

type MiddlewareHandlers struct {
	prevStageSub               middleware.MessageMiddlewareQueue // consider a queue
	nextStagePubs              map[string]middleware.MessageMiddlewareExchange
	broadcastResultsRequestPub middleware.MessageMiddlewareExchange
	broadcastResultsRequestSub middleware.MessageMiddlewareQueue
}

// handleSignal listens for SIGTERM signal and triggers shutdown.
func (f *FilterGenericWorker) handleSignal() {
	<-f.sigChan
	f.log.Info("Handling signal")
	f.Shutdown()
}

func NewFilterGenericWorker(
	rabbitConf middleware.RabbitConfig,
	config FilterConfig,
	watchMeshConfig watch_mesh.WatchMeshConfig,
) (*FilterGenericWorker, error) {
	log := logger.GetLoggerWithPrefix("[FILTER" + config.id + "] ")

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

	return &FilterGenericWorker{
		log:               log,
		middlewareHandler: middlewareHandler,
		sigChan:           sigChan,
		isRunning:         true,
		filter:            *NewFilter(),
		conf:              config,
		errChan:           make(chan middleware.MessageMiddlewareError, ERROR_CHANNEL_BUFFER_SIZE),
		clientsStatsMutex: sync.Mutex{},
		clientsStats:      make(map[ClientId]*middleware.ClientStats),
		resultsChans:      make(map[ClientId]map[DataType]chan middleware.MessageResultsResponse),
		watchMesh:         watch_mesh.NewWatchMesh(watchMeshConfig),
	}, nil
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

func (f *FilterGenericWorker) filterMessage(message amqp.Delivery) error {
	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	if f.conf.ofType == FILTER_TYPE_AMOUNT {
		msg.QueryId = 1
	}

	if msg.IsEof {
		go f.handleEofMessage(message, *msg)
		return nil
	}

	filteredBatch := f.conf.messageCallback(&f.filter, msg.Payload)

	if len(filteredBatch) == 0 {
		// f.log.Info("No transaction passed the filterMessage of type " + f.conf.ofType)
		f.getClientStats(msg.ClientId).Add(msg.DataType, true, false)
		answerMessage(ACK, message)
		return nil
	}

	response := middleware.NewMessageWithPayload(msg.DataType, msg.ClientId, filteredBatch, false, msg.QueryId)
	err = f.sendNextStage(*response)
	if err != nil {
		f.log.Errorf("Failed to send message to next stage: %v", err)
		answerMessage(NACK_REQUEUE, message)
		return err
	}

	f.getClientStats(msg.ClientId).Add(msg.DataType, true, true)

	answerMessage(ACK, message)
	// f.log.Infof("Filtered message and sent to next stage")
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

func (f *FilterGenericWorker) getClientStats(clientId ClientId) *middleware.ClientStats {
	if _, exists := f.clientsStats[clientId]; !exists {
		f.clientsStats[clientId] = middleware.NewClientStats()
	}
	return f.clientsStats[clientId]
}

func (f *FilterGenericWorker) broadcastAndWaitForResults(requestBytes []byte, clientId ClientId, dataType DataType, expectedEmitted int) (processed int, emitted int, timeout bool) {
	for retriesCount := 0; processed < expectedEmitted && retriesCount < middleware.MAX_EOF_RETRIES; retriesCount++ {
		processed = 0
		emitted = 0
		timeout = false
		timeoutDuration := time.Second * time.Duration(middleware.RESPONSE_TIMEOUT_SEC*(retriesCount+1))
		if sendErr := f.middlewareHandlers.broadcastResultsRequestPub.Send(requestBytes); sendErr != middleware.MessageMiddlewareSuccess {
			f.log.Errorf("Failed to send results request message to broadcast exchange: %v", sendErr)
			break
		}
		f.log.Infof("Sent results request message to broadcast exchange for client %s and dataType %s. Attempt %d/%d", clientId, dataType, retriesCount+1, middleware.MAX_EOF_RETRIES)
		for !timeout && processed < expectedEmitted {
			select {
			case msg := <-f.resultsChans[clientId][dataType]:
				f.log.Infof("Received results response from %s for client %s and datatype %s: processed=%d, emitted=%d", msg.Origin, msg.ClientId, msg.DataType, msg.Processed, msg.Emitted)
				processed += msg.Processed
				emitted += msg.Emitted
			case <-time.After(timeoutDuration):
				f.log.Warningf("Timeout waiting for results response for client %s and datatype %s after %d seconds", clientId, dataType, middleware.RESPONSE_TIMEOUT_SEC)
				timeout = true
			}
		}
	}
	return processed, emitted, timeout
}

func (f *FilterGenericWorker) handleEofMessage(eofMessage amqp.Delivery, eofMsg middleware.Message) {
	f.log.Infof("Received EOF message for client %s and dataType %s. Expecting %d processed messages", eofMsg.ClientId, eofMsg.DataType, eofMsg.TotalEmitted)
	mh, err := middleware.NewMiddlewareHandler(f.middlewareHandler.RabbitConn)
	if err != nil {
		f.log.Errorf("Failed to create middleware handler: %v", err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}
	queueName := fmt.Sprintf("filter.%s.results.request.gather.%s", f.conf.ofType, eofMsg.ClientId)
	queue, err := mh.CreateQueue(queueName)
	if err != nil {
		f.log.Errorf("Failed to create ephemeral queue %s: %v", queueName, err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}

	requestMsg := middleware.NewGatherResultsRequest(f.conf.id, queueName, eofMsg.ClientId, eofMsg.DataType)
	requestBytes, err := requestMsg.ToBytes()
	if err != nil {
		f.log.Errorf("Failed to serialize results request message: %v", err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}

	f.ensureResultsChanExists(eofMsg.ClientId, eofMsg.DataType)
	queue.StartConsuming(f.processResultsResponse, f.errChan)

	processed, emitted, timeout := f.broadcastAndWaitForResults(requestBytes, eofMsg.ClientId, eofMsg.DataType, eofMsg.TotalEmitted)
	if processed == 0 {
		f.log.Errorf("Unexpected error waiting results for client %s and dataType %s", eofMsg.ClientId, eofMsg.DataType)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}
	if timeout {
		f.log.Warningf("Could not gather all results for client %s and dataType %s after %d retries. Proceeding with partial results: processed %d/%d, emitted %d", eofMsg.ClientId, eofMsg.DataType, middleware.MAX_EOF_RETRIES, processed, eofMsg.TotalEmitted, emitted)
	}
	if err := queue.StopConsuming(); err != middleware.MessageMiddlewareSuccess {
		f.log.Warningf("Failed to stop consuming ephemeral queue %s: %v", queueName, err)
	}
	if err := queue.Delete(); err != middleware.MessageMiddlewareSuccess {
		f.log.Warningf("Failed to delete ephemeral queue %s: %v", queueName, err)
	}

	expectedTotal := eofMsg.TotalEmitted
	eofMsg.TotalEmitted = emitted
	if err := f.sendNextStage(eofMsg); err != nil {
		f.log.Errorf("Failed to send EOF message to next stage: %v. Requeuing message...", err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}

	f.log.Infof("Sent EOF message to next stage for client %s and dataType %s. Processed count: %d/%d, Emitted count: %d", eofMsg.ClientId, eofMsg.DataType, processed, expectedTotal, emitted)
	answerMessage(ACK, eofMessage)

	clearMsg := middleware.NewClearResultsRequest(f.conf.id, "", eofMsg.ClientId, eofMsg.DataType)
	clearMsgBytes, err := clearMsg.ToBytes()
	if err != nil {
		f.log.Warningf("Failed to serialize results request message: %v", err)
	}
	if sendErr := f.middlewareHandlers.broadcastResultsRequestPub.Send(clearMsgBytes); sendErr != middleware.MessageMiddlewareSuccess {
		f.log.Warningf("Failed to send results request message to broadcast exchange: %v", sendErr)
	}
}

func (f *FilterGenericWorker) processResultsResponse(message amqp.Delivery) error {
	msg, err := middleware.NewMessageResultsResponseFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	f.log.Infof("Received results response from %s for client %s and datatype %s: processed=%d, emitted=%d", msg.Origin, msg.ClientId, msg.DataType, msg.Processed, msg.Emitted)
	f.resultsChans[msg.ClientId][msg.DataType] <- *msg
	answerMessage(ACK, message)
	return nil
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

func (f *FilterGenericWorker) ensureResultsChanExists(clientId ClientId, dataType DataType) {
	if _, exists := f.resultsChans[clientId]; !exists {
		f.resultsChans[clientId] = make(map[DataType]chan middleware.MessageResultsResponse)
	}
	if _, exists := f.resultsChans[clientId][dataType]; !exists {
		f.resultsChans[clientId][dataType] = make(chan middleware.MessageResultsResponse)
	}
}

func (f *FilterGenericWorker) sendResultsRequest(message amqp.Delivery) error {
	msg, err := middleware.NewMessageResultsRequestFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	if msg.RequestType == middleware.RESULTS_REQUEST_TYPE_CLEAR {
		f.getClientStats(msg.ClientId).Clear(msg.DataType)
		f.log.Infof("Cleared stats for client %s and datatype %s", msg.ClientId, msg.DataType)
		answerMessage(ACK, message)
		return nil
	}

	f.log.Infof("Received results request message from %s for client %s and datatype %s", msg.Origin, msg.ClientId, msg.DataType)
	processed, emitted := f.getClientStats(msg.ClientId).GetStats(msg.DataType)

	responseMsg := middleware.MessageResultsResponse{
		Origin:    f.conf.id,
		ClientId:  msg.ClientId,
		DataType:  msg.DataType,
		Processed: processed,
		Emitted:   emitted,
	}

	responseBytes, err := responseMsg.ToBytes()
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	if sendErr := f.SendToQueue(msg.QueueName, responseBytes); sendErr != middleware.MessageMiddlewareSuccess {
		answerMessage(NACK_REQUEUE, message)
		return fmt.Errorf("failed to send results response to queue %s", msg.QueueName)
	}
	f.log.Infof("Sent results response to %s for client %s and datatype %s: processed=%d, emitted=%d", msg.QueueName, msg.ClientId, msg.DataType, processed, emitted)
	answerMessage(ACK, message)
	return nil
}

func (f *FilterGenericWorker) Run() error {
	defer f.Shutdown()
	go f.handleSignal()

	f.watchMesh.Start()

	err := f.createExchangeHandlers()
	if err != nil {
		return fmt.Errorf("failed to create exchange handlers: %v", err)
	}

	f.middlewareHandlers.prevStageSub.StartConsuming(f.filterMessage, f.errChan)
	f.middlewareHandlers.broadcastResultsRequestSub.StartConsuming(f.sendResultsRequest, f.errChan)

	for err := range f.errChan {
		if err != middleware.MessageMiddlewareSuccess {
			f.log.Errorf("Error found while filtering message of type: %v", err)
		}

		if !f.isRunning {
			f.log.Info("Inside error loop: breaking")
			break
		}
	}

	f.log.Info("Finished filtering")
	return nil
}

// Shutdown gracefully stops the acceptor, closing the listener and current client.
func (f *FilterGenericWorker) Shutdown() {
	f.isRunning = false
	f.errChan <- middleware.MessageMiddlewareSuccess
	f.middlewareHandler.Close()

	f.middlewareHandlers.prevStageSub.Close()
	f.middlewareHandlers.broadcastResultsRequestPub.Close()
	f.middlewareHandlers.broadcastResultsRequestSub.Close()
	for _, nextStagePub := range f.middlewareHandlers.nextStagePubs {
		nextStagePub.Close()
	}

	f.log.Info("Shutdown complete")
}
