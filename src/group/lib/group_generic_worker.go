package group

import (
	"common/logger"
	"common/middleware"
	"fmt"
	"group/structures"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type MiddlewareHandlers struct {
	prevStageSub               middleware.MessageMiddlewareQueue
	nextStagePub               middleware.MessageMiddlewareExchange
	broadcastResultsRequestPub middleware.MessageMiddlewareExchange
	broadcastResultsRequestSub middleware.MessageMiddlewareQueue
}
type ClientId = string
type DataType = string

type GroupByGenericWorker struct {
	log               *logging.Logger
	middlewareHandler *middleware.MiddlewareHandler
	sigChan           chan os.Signal
	isRunning         bool

	exchangeHandlers MiddlewareHandlers
	errChan          chan middleware.MessageMiddlewareError

	conf GroupByConfig

	mutex           sync.Mutex
	middlewareMutex sync.Mutex
	group           structures.GrouperPerClient[structures.AllowedGroup]
	// new eof
	clientsStats       map[ClientId]*middleware.ClientStats
	gatherResultsChans map[ClientId]chan int // to signal when a result has been gathered
	resultsChans       map[ClientId]map[DataType]chan middleware.MessageResultsResponse
}

func NewGroupByGenericWorker(rabbitConf middleware.RabbitConfig, conf GroupByConfig) (*GroupByGenericWorker, error) {
	log := logger.GetLoggerWithPrefix("[GROUP-" + conf.id + "] ")

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

	return &GroupByGenericWorker{
		log:               log,
		middlewareHandler: middlewareHandler,
		sigChan:           sigChan,
		isRunning:         true,
		errChan:           make(chan middleware.MessageMiddlewareError, ERROR_CHANNEL_BUFFER_SIZE),
		conf:              conf,

		mutex:           sync.Mutex{},
		middlewareMutex: sync.Mutex{},
		group:           structures.NewGrouperPerClient[structures.AllowedGroup](),

		clientsStats:       make(map[ClientId]*middleware.ClientStats),
		gatherResultsChans: make(map[ClientId]chan int),
		resultsChans:       make(map[ClientId]map[DataType]chan middleware.MessageResultsResponse),
	}, nil
}

// handleSignal listens for SIGTERM signal and triggers shutdown.
func (g *GroupByGenericWorker) handleSignal() {
	<-g.sigChan
	g.log.Info("Handling signal")
	g.Shutdown()
}

func (g *GroupByGenericWorker) getClientStats(clientId ClientId) *middleware.ClientStats {
	if _, exists := g.clientsStats[clientId]; !exists {
		g.clientsStats[clientId] = middleware.NewClientStats()
	}
	return g.clientsStats[clientId]
}

func (g *GroupByGenericWorker) groupMessage(message amqp.Delivery) error {
	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	if msg.IsEof {
		g.log.Infof("EOF received for client %s and datatype %s", msg.ClientId, msg.DataType)
		go g.handleEofMessage(message, *msg)
		return nil
	}

	g.mutex.Lock()
	g.group.Add(msg.ClientId, msg.Payload, g.conf.factory)
	g.mutex.Unlock()

	g.getClientStats(msg.ClientId).Add(msg.DataType, true, false)

	answerMessage(ACK, message)
	// g.log.Info("Grouped message")
	return nil
}

func (g *GroupByGenericWorker) sendNextStage(msgToSend middleware.MessageGrouped) error {
	msgBytes, err := msgToSend.ToBytes()
	if err != nil {
		return err
	}
	g.log.Infof("Sending message to next stage of bytes size %d", len(msgBytes))
	g.middlewareMutex.Lock()
	sendErr := g.exchangeHandlers.nextStagePub.Send(msgBytes)
	g.middlewareMutex.Unlock()
	if sendErr != 0 {
		return fmt.Errorf("error sending message to next stage: %d", sendErr)
	}
	return nil
}

func (g *GroupByGenericWorker) sendEofNextStage(msgToSend middleware.Message) error {
	msgBytes, err := msgToSend.ToBytes()
	if err != nil {
		return err
	}
	g.middlewareMutex.Lock()
	g.exchangeHandlers.nextStagePub.Send(msgBytes)
	g.middlewareMutex.Unlock()
	return nil
}

func (g *GroupByGenericWorker) ensureResultsChanExists(clientId ClientId, dataType DataType) {
	if _, exists := g.resultsChans[clientId]; !exists {
		g.resultsChans[clientId] = make(map[DataType]chan middleware.MessageResultsResponse)
	}
	if _, exists := g.resultsChans[clientId][dataType]; !exists {
		g.resultsChans[clientId][dataType] = make(chan middleware.MessageResultsResponse)
	}
}

func (g *GroupByGenericWorker) processResultsResponse(message amqp.Delivery) error {
	msg, err := middleware.NewMessageResultsResponseFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	g.log.Infof("Received results response from %s for client %s and datatype %s: processed=%d, emitted=%d", msg.Origin, msg.ClientId, msg.DataType, msg.Processed, msg.Emitted)
	g.resultsChans[msg.ClientId][msg.DataType] <- *msg
	answerMessage(ACK, message)
	return nil
}

func (g *GroupByGenericWorker) handleEofMessage(eofMessage amqp.Delivery, eofMsg middleware.Message) {
	g.log.Infof("Received EOF message for client %s and dataType %s. Expecting %d processed messages", eofMsg.ClientId, eofMsg.DataType, eofMsg.TotalEmitted)
	mh, err := middleware.NewMiddlewareHandler(g.middlewareHandler.RabbitConn)
	if err != nil {
		g.log.Errorf("Failed to create middleware handler: %v", err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}
	queueName := fmt.Sprintf("group.%s.results.request.gather.%s", g.conf.ofType, eofMsg.ClientId)
	queue, err := mh.CreateQueue(queueName)
	if err != nil {
		g.log.Errorf("Failed to create ephemeral queue %s: %v", queueName, err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}

	g.ensureResultsChanExists(eofMsg.ClientId, eofMsg.DataType)
	queue.StartConsuming(g.processResultsResponse, g.errChan)

	// GATHER AND SEND
	gatherMsg := middleware.NewGatherResultsRequest(g.conf.id, queueName, eofMsg.ClientId, eofMsg.DataType)
	gatherBytes, err := gatherMsg.ToBytes()
	if err != nil {
		g.log.Errorf("Failed to serialize results request message: %v", err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}
	processed, _, results, timeout := g.broadcastAndWaitForResults(gatherBytes, eofMsg.ClientId, eofMsg.DataType, eofMsg.TotalEmitted)
	if processed == 0 {
		g.log.Errorf("Unexpected error waiting results for client %s and dataType %s", eofMsg.ClientId, eofMsg.DataType)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}
	if timeout {
		g.log.Warningf("Could not gather all stats results for client %s and dataType %s after %d retries. Proceeding with partial results: processed %d/%d", eofMsg.ClientId, eofMsg.DataType, middleware.MAX_EOF_RETRIES, processed, eofMsg.TotalEmitted)
	}

	messageToSend := results.GetMessageToSend()
	var emitted int = 0

	emitted = 0
	for _, group := range messageToSend {
		response := middleware.NewMessageGrouped(eofMsg.DataType, eofMsg.ClientId, group, false, eofMsg.QueryId)

		middleError := g.sendNextStage(*response)
		if middleError != nil {
			g.log.Errorf("problem while sending message to %s: %v", g.conf.nextStagePub, middleError)
			continue
		}
		g.log.Infof("Sent consolidated results")
		emitted++
	}

	eofMsg.TotalEmitted = emitted
	err = g.sendEofNextStage(eofMsg)
	if err != nil {
		g.log.Errorf("Failed to send EOF message to next stage: %v", err)
		answerMessage(NACK_DISCARD, eofMessage)
		return
	}
	answerMessage(ACK, eofMessage)
	g.log.Infof("Sent EOF message to next stage for client %s and dataType %s. Emitted count: %d", eofMsg.ClientId, eofMsg.DataType, eofMsg.TotalEmitted)

	// CLEAN
	clearMsg := middleware.NewClearResultsRequest(g.conf.id, "", eofMsg.ClientId, eofMsg.DataType)
	clearMsgBytes, err := clearMsg.ToBytes()
	if err != nil {
		g.log.Warningf("Failed to serialize results request message: %v", err)
	}
	if sendErr := g.exchangeHandlers.broadcastResultsRequestPub.Send(clearMsgBytes); sendErr != middleware.MessageMiddlewareSuccess {
		g.log.Warningf("Failed to send results request message to broadcast exchange: %v", sendErr)
	}
}

func (g *GroupByGenericWorker) broadcastAndWaitForResults(requestBytes []byte, clientId ClientId, dataType DataType, expectedEmitted int) (processed, emitted int, results structures.AllowedGroup, timeout bool) {
	g.middlewareMutex.Lock()
	g.exchangeHandlers.broadcastResultsRequestPub.Send(requestBytes)
	g.middlewareMutex.Unlock()

	results = g.conf.factory()

	for retriesCount := 0; processed < expectedEmitted && retriesCount < middleware.MAX_EOF_RETRIES; retriesCount++ {
		processed = 0
		emitted = 0
		timeout = false
		timeoutDuration := time.Second * time.Duration(middleware.RESPONSE_TIMEOUT_SEC*(retriesCount+1))
		if sendErr := g.exchangeHandlers.broadcastResultsRequestPub.Send(requestBytes); sendErr != middleware.MessageMiddlewareSuccess {
			g.log.Errorf("Failed to send results request message to broadcast exchange: %v", sendErr)
			break
		}
		g.log.Infof("Sent results request message to broadcast exchange for client %s and dataType %s. Attempt %d/%d", clientId, dataType, retriesCount+1, middleware.MAX_EOF_RETRIES)
		for !timeout && processed < expectedEmitted {
			select {
			case msg := <-g.resultsChans[clientId][dataType]:
				processed += msg.Processed
				emitted += msg.Emitted
				// TODO: simplify merging interface
				incomingGroup := g.conf.factory()
				incomingGroup.FromMapString(msg.GroupedPayload)
				results.Merge(incomingGroup)
			case <-time.After(timeoutDuration):
				g.log.Warningf("Timeout waiting for results response for client %s and datatype %s after %d seconds", clientId, dataType, middleware.RESPONSE_TIMEOUT_SEC)
				timeout = true
			}
		}
	}
	return processed, emitted, results, timeout
}

func (g *GroupByGenericWorker) gatherAndMergePartialResults(message amqp.Delivery) error {

	msg, err := middleware.NewMessageGroupedFromBytes(message.Body)
	if err != nil {
		g.log.Errorf("Failed to parse message: %v", err)
		answerMessage(NACK_DISCARD, message)
		return err
	}
	g.log.Infof("Gathering partial results for client %s and dataType %s", msg.ClientId, msg.DataType)

	partialGrouping := g.conf.factory()
	partialGrouping.FromMapString(msg.Payload)

	g.mutex.Lock()
	currentGroup := g.group.Get(msg.ClientId, g.conf.factory)
	currentGroup.Merge(partialGrouping)
	g.mutex.Unlock()

	g.log.Infof("Partial results merged for client %s and dataType %s", msg.ClientId, msg.DataType)
	// Signal that a result has been gathered
	if ch, exists := g.gatherResultsChans[msg.ClientId]; exists {
		ch <- 1
	}
	answerMessage(ACK, message)
	return nil
}

func (g *GroupByGenericWorker) gatherOtherPartialResults(eofMessage amqp.Delivery, eofMsg middleware.Message) {
	countToWaitResults := g.conf.count - 1
	if countToWaitResults <= 0 {
		g.log.Infof("No need to gather other partial results, only one instance for client %s and dataType %s", "", "")
		return
	}

	middlewareHandler, err := middleware.NewMiddlewareHandler(g.middlewareHandler.RabbitConn)
	if err != nil {
		g.log.Errorf("Failed to create middleware handler: %v", err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}

	// Create Ephemeral queue
	queueName := fmt.Sprintf("group.%s.results.request.gather.%s", g.conf.ofType, eofMsg.ClientId)
	g.middlewareMutex.Lock()
	queue, err := middlewareHandler.CreateQueue(queueName)
	g.middlewareMutex.Unlock()

	if err != nil {
		g.log.Errorf("Failed to declare ephemeral queue: %v", err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}
	g.log.Infof("Requesting results to receive in queue %s for client %s and dataType %s", queueName, eofMsg.ClientId, eofMsg.DataType)
	requestMsg := middleware.NewMessageResultsRequest(g.conf.id, queueName, eofMsg.ClientId, eofMsg.DataType)
	requestBytes, err := requestMsg.ToBytes()
	if err != nil {
		g.log.Errorf("Failed to serialize message: %v", err)
		answerMessage(NACK_REQUEUE, eofMessage)
		return
	}

	// Broadcast results request
	g.middlewareMutex.Lock()
	g.exchangeHandlers.broadcastResultsRequestPub.Send(requestBytes)
	g.middlewareMutex.Unlock()

	// Create channel to gather results
	g.gatherResultsChans[eofMsg.ClientId] = make(chan int, countToWaitResults)
	// Consume from ephemeral queue
	g.log.Infof("Consuming results from queue %s for client %s and dataType %s", queueName, eofMsg.ClientId, eofMsg.DataType)
	queue.StartConsuming(g.gatherAndMergePartialResults, g.errChan)

	for i := range countToWaitResults {
		g.log.Infof("Waiting for partial results %d/%d for client %s and dataType %s", i+1, countToWaitResults, eofMsg.ClientId, eofMsg.DataType)
		<-g.gatherResultsChans[eofMsg.ClientId]
		g.log.Infof("Received partial results %d/%d for client %s and dataType %s", i+1, countToWaitResults, eofMsg.ClientId, eofMsg.DataType)
	}
	// Stop consuming and delete ephemeral queue
	queue.StopConsuming()
	queue.Delete()
	g.log.Infof("All partial results received for client %s and dataType %s", eofMsg.ClientId, eofMsg.DataType)
	delete(g.gatherResultsChans, eofMsg.ClientId)
}

func (g *GroupByGenericWorker) gatherResultsAndSendEof(eofMessage amqp.Delivery, eofMsg middleware.Message, clientStats *middleware.ClientStats) {
	g.gatherOtherPartialResults(eofMessage, eofMsg)

	// SEND RESULTS
	g.mutex.Lock()
	currentGroup := g.group.Get(eofMsg.ClientId, g.conf.factory)
	g.mutex.Unlock()

	messageToSend := currentGroup.GetMessageToSend()
	var emitted int = 0

	emitted = 0
	for _, group := range messageToSend {
		response := middleware.NewMessageGrouped(eofMsg.DataType, eofMsg.ClientId, group, false, eofMsg.QueryId)

		middleError := g.sendNextStage(*response)
		if middleError != nil {
			g.log.Errorf("problem while sending message to %s: %v", g.conf.nextStagePub, middleError)
			continue
		}
		g.log.Infof("Sent consolidated results")
		emitted++
	}

	// update emitted count and send eof
	eofMsg.TotalEmitted = emitted
	err := g.sendEofNextStage(eofMsg)
	if err != nil {
		g.log.Errorf("Failed to send EOF message to next stage: %v", err)
		answerMessage(NACK_DISCARD, eofMessage)
		return
	}

	answerMessage(ACK, eofMessage)
	g.log.Infof("Sent EOF message to next stage for client %s and dataType %s. Emitted count: %d", eofMsg.ClientId, eofMsg.DataType, eofMsg.TotalEmitted)

	// DELETE AFTER SENDING
	g.mutex.Lock()
	g.group.Delete(eofMsg.ClientId)
	g.mutex.Unlock()

}

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
		broadcastResultsRequestPub: *broadcastResultsRequestPub,
		broadcastResultsRequestSub: *broadcastResultsRequestSub,
	}

	return nil
}

// TODO: no hace falta diferenciar entre count y gather, se puede hacer todo en gather
func (g *GroupByGenericWorker) sendResultsRequest(message amqp.Delivery) error {
	msg, err := middleware.NewMessageResultsRequestFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}
	// g.log.Infof("Received request to gather and send partial results from %s to queue %s", msg.Origin, msg.QueueName)
	// if msg.Origin == g.conf.id {
	// 	g.log.Infof("Ignoring request to gather and send partial results from myself %s", msg.Origin)
	// 	answerMessage(ACK, message)
	// 	return nil
	// }

	clientStats := g.getClientStats(msg.ClientId)

	if msg.RequestType == middleware.RESULTS_REQUEST_TYPE_CLEAR {
		g.log.Infof("Clearing stored group for client %s as per request from %s", msg.ClientId, msg.Origin)
		g.mutex.Lock()
		g.group.Delete(msg.ClientId)
		g.mutex.Unlock()
		clientStats.Clear(msg.DataType)
		answerMessage(ACK, message)
		return nil
	}

	g.log.Infof("Gathering and sending grouped payload to %s for client %s and datatype %s", msg.QueueName, msg.ClientId, msg.DataType)
	g.mutex.Lock()
	currentGroup := g.group.Get(msg.ClientId, g.conf.factory)
	g.mutex.Unlock()
	processed, emitted := clientStats.GetStats(msg.DataType)
	// TODO: batch results if too large
	// Send processed 0 and emitted 0 to indicate that results are being sent
	// Send real processed and emitted in last message
	responseMsg := middleware.MessageResultsResponse{
		Origin:         g.conf.id,
		ClientId:       msg.ClientId,
		DataType:       msg.DataType,
		Processed:      processed,
		Emitted:        emitted,
		GroupedPayload: currentGroup.ToMapString(),
	}
	responseBytes, err := responseMsg.ToBytes()
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}
	if sendErr := g.SendToQueue(msg.QueueName, responseBytes); sendErr != middleware.MessageMiddlewareSuccess {
		answerMessage(NACK_REQUEUE, message)
		return fmt.Errorf("failed to send results response to queue %s", msg.QueueName)
	}
	g.log.Infof("Sent results response to %s for client %s and datatype %s: processed=%d, emitted=%d", msg.QueueName, msg.ClientId, msg.DataType, processed, emitted)
	answerMessage(ACK, message)
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

func (g *GroupByGenericWorker) Run() error {
	defer g.Shutdown()
	go g.handleSignal()

	err := g.createExchangeHandlers()
	if err != nil {
		return fmt.Errorf("failed to create exchange handlers: %v", err)
	}

	g.log.Infof("Starting to consume messages from %s", g.conf.prevStageSub)
	g.exchangeHandlers.prevStageSub.StartConsuming(g.groupMessage, g.errChan)
	g.exchangeHandlers.broadcastResultsRequestSub.StartConsuming(g.sendResultsRequest, g.errChan)

	for err := range g.errChan {
		if err != middleware.MessageMiddlewareSuccess {
			g.log.Errorf("Error found while grouping by Yearmonth message of type: %v", err)
		}

		if !g.isRunning {
			g.log.Info("Inside error loop: breaking")
			break
		}
	}

	g.log.Info("Finished grouping")
	return nil
}

// Shutdown gracefully stops the acceptor, closing the listener and current client.
func (g *GroupByGenericWorker) Shutdown() {
	g.isRunning = false
	g.errChan <- middleware.MessageMiddlewareSuccess

	g.exchangeHandlers.prevStageSub.StopConsuming()
	g.exchangeHandlers.prevStageSub.Close()
	g.exchangeHandlers.nextStagePub.Close()
	g.exchangeHandlers.broadcastResultsRequestSub.StopConsuming()
	g.exchangeHandlers.broadcastResultsRequestSub.Close()
	g.middlewareHandler.Close()

	g.log.Info("Shutdown complete")
}
