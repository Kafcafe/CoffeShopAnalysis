package group

import (
	"common/logger"
	"common/middleware"
	"common/watch_mesh"
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

const REQUEUE_PROBABILITY = 0.1

const BATCH_SIZE_GROUPED_MESSAGE = 1000

type MiddlewareHandlers struct {
	prevStageSub               middleware.MessageMiddlewareQueue
	nextStagePub               middleware.MessageMiddlewareExchange
	privateQueueSub            middleware.MessageMiddlewareQueue
	privateQueuesPub           map[int]*middleware.MessageMiddlewareQueue
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
	clientsStats map[ClientId]*middleware.ClientStats
	resultsChans map[ClientId]map[DataType]chan middleware.MessageResultsResponse
	watchMesh    *watch_mesh.WatchMesh
	cache        *middleware.Cache
}

func NewGroupByGenericWorker(
	rabbitConf middleware.RabbitConfig,
	conf GroupByConfig,
	watchMeshConfig watch_mesh.WatchMeshConfig,
) (*GroupByGenericWorker, error) {
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

		clientsStats: make(map[ClientId]*middleware.ClientStats),
		resultsChans: make(map[ClientId]map[DataType]chan middleware.MessageResultsResponse),
		watchMesh:    watch_mesh.NewWatchMesh(watchMeshConfig),
		cache:        middleware.NewCache(middleware.DEFAULT_CACHE_CAPACITY),
	}, nil
}

// handleSignal listens for SIGTERM signal and triggers shutdown.
func (g *GroupByGenericWorker) handleSignal() {
	<-g.sigChan
	g.log.Info("Handling signal")
	g.Shutdown()
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

	// TODO: change to id num
	privateQueueName := fmt.Sprintf("group.%s.private.%s", g.conf.ofType, g.conf.id)
	g.log.Infof("Creating private queue handler for group %s: %s", g.conf.id, privateQueueName)
	privateQueueSub, err := g.middlewareHandler.CreateQueue(privateQueueName)
	if err != nil {
		return fmt.Errorf("error creating private queue for group %s: %v", g.conf.id, err)
	}

	privateQueuesPub := make(map[int]*middleware.MessageMiddlewareQueue)
	for i := range g.conf.count {
		// TODO: change to id num
		id := "-" + g.conf.ofType + fmt.Sprintf("%d", i+1)
		privateQueuePubName := fmt.Sprintf("group.%s.private.%s", g.conf.ofType, id)
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

func (g *GroupByGenericWorker) getClientStats(clientId ClientId) *middleware.ClientStats {
	if _, exists := g.clientsStats[clientId]; !exists {
		g.clientsStats[clientId] = middleware.NewClientStats()
	}
	return g.clientsStats[clientId]
}

func (g *GroupByGenericWorker) ensureResultsChanExists(clientId ClientId, dataType DataType) {
	if _, exists := g.resultsChans[clientId]; !exists {
		g.resultsChans[clientId] = make(map[DataType]chan middleware.MessageResultsResponse)
	}
	if _, exists := g.resultsChans[clientId][dataType]; !exists {
		g.resultsChans[clientId][dataType] = make(chan middleware.MessageResultsResponse)
	}
}

func (g *GroupByGenericWorker) groupMessage(message amqp.Delivery) error {
	message_id := message.MessageId
	if g.cache.Contains(message_id) {
		g.log.Infof("Message %s already processed", message_id)
		answerMessage(ACK, message)
		return nil
	}

	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	if msg.IsEof {
		go g.handleEofMessage(message, *msg)
		return nil
	}

	g.mutex.Lock()
	g.group.Add(msg.ClientId, msg.Payload, g.conf.factory)
	g.getClientStats(msg.ClientId).Add(msg.DataType, true, false)
	g.mutex.Unlock()
	g.cache.Add(message_id)

	answerMessage(ACK, message)
	return nil
}

func (g *GroupByGenericWorker) sendMessage(msgToSend middleware.Message) error {
	msgBytes, err := msgToSend.ToBytes()
	if err != nil {
		return err
	}
	return g.sendBytesNextStage(msgBytes)
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

func (g *GroupByGenericWorker) processResultsResponse(message amqp.Delivery) error {
	msg, err := middleware.NewMessageResultsResponseFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}
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

	g.log.Infof("Received results for client %s and datatype %s: processed=%d/%d", eofMsg.ClientId, eofMsg.DataType, processed, eofMsg.TotalEmitted)

	messageToSend := results.GetMessageToSend()
	var emitted int = 0
	for _, group := range messageToSend {
		response := middleware.NewMessageWithGroupedPayload(eofMsg.DataType, eofMsg.ClientId, group, false, eofMsg.QueryId)

		middleError := g.sendMessage(*response)
		if middleError != nil {
			g.log.Errorf("problem while sending message to %s: %v", g.conf.nextStagePub, middleError)
			continue
		}
		emitted++
	}

	eofMsg.TotalEmitted = emitted
	err = g.sendMessage(eofMsg)
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
	for retriesCount := 0; retriesCount < middleware.MAX_EOF_RETRIES; retriesCount++ {
		processed = 0
		emitted = 0
		results = g.conf.factory()
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
				if msg.GroupedPayload != nil {
					results.AddMapString(msg.GroupedPayload)
				}
			case <-time.After(timeoutDuration):
				g.log.Warningf("Timeout waiting for results response for client %s and datatype %s after %d seconds. Processed %d/%d", clientId, dataType, middleware.RESPONSE_TIMEOUT_SEC, processed, expectedEmitted)
				timeout = true
			}
		}
		if !timeout {
			break
		}
	}
	return processed, emitted, results, timeout
}

func (g *GroupByGenericWorker) sendResultsRequest(message amqp.Delivery) error {
	msg, err := middleware.NewMessageResultsRequestFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	if msg.RequestType == middleware.RESULTS_REQUEST_TYPE_CLEAR {
		g.log.Infof("Clearing stored group for client %s as per request from %s", msg.ClientId, msg.Origin)
		g.mutex.Lock()
		g.group.Delete(msg.ClientId)
		g.getClientStats(msg.ClientId).Clear(msg.DataType)
		g.mutex.Unlock()
		answerMessage(ACK, message)
		return nil
	}

	g.mutex.Lock()
	processed, emitted := g.getClientStats(msg.ClientId).GetStats(msg.DataType)
	currentMapString := g.group.Get(msg.ClientId, g.conf.factory).ToMapString()
	g.mutex.Unlock()

	g.sendResultsRequestBatched(msg, currentMapString, BATCH_SIZE_GROUPED_MESSAGE)

	responseMsg := middleware.MessageResultsResponse{
		Origin:    g.conf.id,
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
	if sendErr := g.SendToQueue(msg.QueueName, responseBytes); sendErr != middleware.MessageMiddlewareSuccess {
		answerMessage(NACK_REQUEUE, message)
		return fmt.Errorf("failed to send results response to queue %s", msg.QueueName)
	}
	g.log.Infof("Sent results response to %s for client %s and datatype %s: processed=%d, emitted=%d", msg.QueueName, msg.ClientId, msg.DataType, processed, emitted)
	answerMessage(ACK, message)
	return nil
}

func (g *GroupByGenericWorker) sendResultsRequestBatched(msg *middleware.MessageResultsRequest, group map[string][]string, batchSize int) error {
	for key, value := range group {
		chunks := chunkSlice(value, batchSize)
		g.log.Infof("Sending %d chunks", len(chunks))
		for _, chunk := range chunks {
			partialGroup := map[string][]string{key: chunk}
			responseMsg := middleware.MessageResultsResponse{
				Origin:         g.conf.id,
				ClientId:       msg.ClientId,
				DataType:       msg.DataType,
				GroupedPayload: partialGroup,
			}
			responseBytes, err := responseMsg.ToBytes()
			if err != nil {
				return err
			}
			if sendErr := g.SendToQueue(msg.QueueName, responseBytes); sendErr != middleware.MessageMiddlewareSuccess {
				return fmt.Errorf("failed to send results response to queue %s", msg.QueueName)
			}
		}
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

func (g *GroupByGenericWorker) Run() error {
	defer g.Shutdown()
	go g.handleSignal()

	g.watchMesh.Start()

	err := g.createExchangeHandlers()
	if err != nil {
		return fmt.Errorf("failed to create exchange handlers: %v", err)
	}

	g.log.Infof("Starting to consume messages from %s", g.conf.prevStageSub)
	g.exchangeHandlers.privateQueueSub.StartConsuming(g.groupMessage, g.errChan)
	g.exchangeHandlers.prevStageSub.StartConsuming(g.dispatchMessage, g.errChan)
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
