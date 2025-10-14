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

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type MiddlewareHandlers struct {
	prevStageSub               middleware.MessageMiddlewareQueue
	nextStagePub               middleware.MessageMiddlewareExchange
	broadcastCountPub          middleware.MessageMiddlewareExchange
	broadcastCountSub          middleware.MessageMiddlewareQueue
	broadcastResultsRequestPub middleware.MessageMiddlewareExchange
	broadcastResultsRequestSub middleware.MessageMiddlewareQueue
}
type ClientId = string

type GroupByGenericWorker struct {
	log               *logging.Logger
	middlewareHandler *middleware.MiddlewareHandler
	sigChan           chan os.Signal
	isRunning         bool

	exchangeHandlers MiddlewareHandlers
	errChan          chan middleware.MessageMiddlewareError

	conf GroupByConfig

	// currentMessageProcessing  middleware.Message
	mutex sync.Mutex
	// eofChan                   chan int
	eofIntercommunicationChan chan structures.AllowedGroup
	// groupedPerClient          structures.GroupedPerClient
	group structures.GrouperPerClient[structures.AllowedGroup]
	// new eof
	clientsStats       map[ClientId]*middleware.ClientStats
	gatherResultsChans map[ClientId]chan int // to signal when a result has been gathered
}

func NewGroupByGenericWorker(rabbitConf middleware.RabbitConfig, conf GroupByConfig) (*GroupByGenericWorker, error) {
	log := logger.GetLoggerWithPrefix("[GROUP-GEN]")

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

		mutex: sync.Mutex{},
		// eofChan:                   make(chan int, SINGLE_ITEM_BUFFER_LEN),
		eofIntercommunicationChan: make(chan structures.AllowedGroup, SINGLE_ITEM_BUFFER_LEN),
		group:                     structures.NewGrouperPerClient[structures.AllowedGroup](),

		clientsStats:       make(map[ClientId]*middleware.ClientStats),
		gatherResultsChans: make(map[ClientId]chan int),
	}, nil
}

// handleSignal listens for SIGTERM signal and triggers shutdown.
func (g *GroupByGenericWorker) handleSignal() {
	<-g.sigChan
	g.log.Info("Handling signal")
	g.Shutdown()
}

// func (g *GroupByGenericWorker) processInboundEof(message amqp.Delivery) error {
// 	msg, err := middleware.NewEofMessageGroupedFromBytes(message.Body)
// 	if err != nil {
// 		answerMessage(NACK_DISCARD, message)
// 		return err
// 	}
// 	g.log.Warningf("processInboundEof %s groupBy%s", msg.DataType, g.conf.id)

// 	didSomebodyElseAcked := msg.Origin == g.conf.id && msg.IsAck && msg.ImmediateSource != g.conf.id
// 	if didSomebodyElseAcked {
// 		g.log.Infof("Somebody else acked for %s groupBy%s", msg.DataType, g.conf.id)
// 		partialGrouping := g.conf.factory()
// 		partialGrouping.FromMapString(msg.Payload)

// 		g.log.Infof("%v", partialGrouping)
// 		g.eofIntercommunicationChan <- partialGrouping
// 		answerMessage(ACK, message)
// 		return nil
// 	}

// 	isAckMine := msg.ImmediateSource == g.conf.id
// 	isAckForNotForMe := msg.IsAck && msg.Origin != g.conf.id
// 	if isAckMine || isAckForNotForMe {
// 		answerMessage(ACK, message)
// 		return nil
// 	}

// 	g.log.Warning("Lock")
// 	g.mutex.Lock()
// 	currentMessageProcessing := g.currentMessageProcessing
// 	g.mutex.Unlock()
// 	g.log.Warning("Unlock")

// 	if currentMessageProcessing.IsFromSameStream(msg.DataType, msg.ClientId) {
// 		g.log.Warningf("BEFORE INBOUND %s", msg.DataType)
// 		<-g.eofChan
// 		g.log.Warningf("AFTER INBOUND %s", msg.DataType)
// 	}

// 	msg.ImmediateSource = g.conf.id
// 	msg.IsAck = true

// 	g.mutex.Lock()
// 	msg.Payload = g.group.ToMapString(msg.ClientId)
// 	g.mutex.Unlock()

// 	msgBytes, err := msg.ToBytes()
// 	if err != nil {
// 		answerMessage(NACK_DISCARD, message)
// 		return err
// 	}

// 	answerMessage(ACK, message)
// 	g.exchangeHandlers.eofPub.Send(msgBytes)
// 	return nil
// }

// func (g *GroupByGenericWorker) initiateEofCoordination(originalMsg middleware.Message) {
// 	eofMsg := middleware.NewEofMessageGrouped(originalMsg.DataType, originalMsg.ClientId, g.conf.id, g.conf.id, false, nil, originalMsg.QueryId)
// 	msgBytes, err := eofMsg.ToBytes()
// 	if err != nil {
// 		g.log.Errorf("Failed to serialize message: %v", err)
// 	}

// 	g.exchangeHandlers.eofPub.Send(msgBytes)

// 	totalEofs := g.conf.count - 1

// 	if totalEofs == 0 {
// 		g.log.Infof("No EOF coordination needed for %s", originalMsg.DataType)
// 	} else {
// 		g.log.Infof("Coordinating EOF for %s", originalMsg.DataType)
// 	}

// 	g.log.Infof("Consolidating partial results for %s", originalMsg.DataType)

// 	g.mutex.Lock()
// 	currentGroup := g.group.Get(originalMsg.ClientId, g.conf.factory)
// 	g.group.Delete(originalMsg.ClientId)
// 	g.mutex.Unlock()

// 	for i := 0; i < totalEofs; i++ {
// 		g.log.Warningf("BEFORE %d %s", i, originalMsg.DataType)

// 		partialGrouping := <-g.eofIntercommunicationChan

// 		g.log.Infof("%v", partialGrouping)
// 		g.log.Infof("%v", currentGroup)

// 		currentGroup.Merge(partialGrouping)

// 		g.log.Infof("%v", currentGroup)
// 		g.log.Warningf("AFTER %d %s", i, originalMsg.DataType)
// 	}

// 	messageToSend := currentGroup.GetMessageToSend()
// 	emitted := 0
// 	for _, messages := range messageToSend {
// 		for key, records := range messages {
// 			singleYearMonthRecords := map[string][]string{key: records}
// 			response := middleware.NewMessageGrouped(originalMsg.DataType, originalMsg.ClientId, singleYearMonthRecords, false, originalMsg.QueryId)
// 			responseBytes, err := response.ToBytes()
// 			if err != nil {
// 				g.log.Errorf("%v", err)
// 			}

// 			g.log.Infof("Sent consolidated results for year-month top profit: %s", key)

// 			middleError := g.exchangeHandlers.nextStagePub.Send(responseBytes)
// 			emitted++
// 			if middleError != middleware.MessageMiddlewareSuccess {
// 				g.log.Errorf("problem while sending message to %s", g.conf.nextStagePub)
// 			}
// 		}
// 	}

// 	g.log.Infof("Final results grouped and consolidated")

// 	originalMsg.TotalEmitted = emitted
// 	eofMessageBytes, err := originalMsg.ToBytes()
// 	if err != nil {
// 		g.log.Errorf("%v", err)
// 	}
// 	middleError := g.exchangeHandlers.nextStagePub.Send(eofMessageBytes)
// 	if middleError != middleware.MessageMiddlewareSuccess {
// 		g.log.Errorf("problem while propagating EOF")
// 	}

// 	g.log.Warningf("Propagated EOF for %s to next pipeline stage. Total emitted: %d", originalMsg.DataType, originalMsg.TotalEmitted)
// }

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
		clientStats := g.getClientStats(msg.ClientId)
		clientStats.SetEof(msg.DataType, msg.TotalEmitted)
		go g.handleEofMessage(message, *msg)
		return nil
	}

	g.mutex.Lock()
	g.group.Add(msg.ClientId, msg.Payload, g.conf.factory)
	g.mutex.Unlock()

	msgProcessed := middleware.NewMessageProcessed(msg.DataType, msg.ClientId, false, msg.QueryId)
	err = g.sendProcessedMessage(msgProcessed)
	if err != nil {
		g.log.Errorf("Failed to send processed count message: %v", err)
		answerMessage(NACK_REQUEUE, message)
		return err
	}

	answerMessage(ACK, message)
	g.log.Info("Grouped message")
	return nil
}

func (g *GroupByGenericWorker) sendNextStage(msgToSend middleware.MessageGrouped) error {
	msgBytes, err := msgToSend.ToBytes()
	if err != nil {
		return err
	}
	sendErr := g.exchangeHandlers.nextStagePub.Send(msgBytes)
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
	g.exchangeHandlers.nextStagePub.Send(msgBytes)
	return nil
}

func (g *GroupByGenericWorker) handleEofMessage(eofMessage amqp.Delivery, eofMsg middleware.Message) {
	clientStats := g.getClientStats(eofMsg.ClientId)

	g.log.Infof("Received EOF message for client %s and dataType %s. Expecting %d processed messages", eofMsg.ClientId, eofMsg.DataType, eofMsg.TotalEmitted)

	if clientStats.GetProcessed(eofMsg.DataType) < eofMsg.TotalEmitted {
		g.log.Infof("Not all messages processed yet for client %s and dataType %s", eofMsg.ClientId, eofMsg.DataType)
		g.log.Infof("Waiting for all messages to be processed for client %s and dataType %s", eofMsg.ClientId, eofMsg.DataType)
		clientStats.WaitForEofChan(eofMsg.DataType)
	}

	g.log.Infof("All messages processed for client %s and dataType %s", eofMsg.ClientId, eofMsg.DataType)

	g.log.Infof("Initiating gathering results and sending EOF for client %s and dataType %s", eofMsg.ClientId, eofMsg.DataType)
	g.gatherResultsAndSendEof(eofMessage, eofMsg, clientStats)
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

	// Create Ephemeral queue
	queueName := fmt.Sprintf("group.%s.results.request.gather.%s", g.conf.ofType, eofMsg.ClientId)
	queue, err := g.middlewareHandler.CreateQueue(queueName)
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
	g.exchangeHandlers.broadcastResultsRequestPub.Send(requestBytes)

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
	emitted := 0
	for _, messages := range messageToSend {
		for key, records := range messages {
			keyRecords := map[string][]string{key: records}
			response := middleware.NewMessageGrouped(eofMsg.DataType, eofMsg.ClientId, keyRecords, false, eofMsg.QueryId)

			middleError := g.sendNextStage(*response)
			if middleError != nil {
				g.log.Errorf("problem while sending message to %s: %v", g.conf.nextStagePub, middleError)
				continue
			}
			g.log.Infof("Sent consolidated results: %s", key)
			emitted++
		}
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

	g.log.Infof("Setting up count PUB for group %s", g.conf.id)
	broadcastCountPubRoutKey := fmt.Sprintf("group.%s.count", g.conf.ofType)
	broadcastCountPub, err := g.middlewareHandler.CreateFanoutExchangeStandalone(broadcastCountPubRoutKey)
	if err != nil {
		return fmt.Errorf("error creating exchange handler for %s: %v", broadcastCountPubRoutKey, err)
	}

	g.log.Infof("Setting up count SUB for group %s", g.conf.id)
	broadcastCountSubQueueName := fmt.Sprintf("group.%s.count.%s", g.conf.ofType, g.conf.id)
	broadcastCountSub, err := g.middlewareHandler.CreateQueue(broadcastCountSubQueueName)
	if err != nil {
		return fmt.Errorf("error creating count queue for %s: %v", broadcastCountSubQueueName, err)
	}
	err = g.middlewareHandler.BindQueue(broadcastCountSubQueueName, broadcastCountPubRoutKey, "")

	if err != nil {
		return fmt.Errorf("error preparing count queue for %s: %v", g.conf.ofType, err)
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
		broadcastCountPub:          *broadcastCountPub,
		broadcastCountSub:          *broadcastCountSub,
		broadcastResultsRequestPub: *broadcastResultsRequestPub,
		broadcastResultsRequestSub: *broadcastResultsRequestSub,
	}

	return nil
}

func (g *GroupByGenericWorker) sendProcessedMessage(msgProcessed *middleware.MessageProcessed) error {
	msgProcessedBytes, err := msgProcessed.ToBytes()
	if err != nil {
		return err
	}
	sendErr := g.exchangeHandlers.broadcastCountPub.Send(msgProcessedBytes)
	if sendErr != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to send processed count message: %v", sendErr)
	}
	return nil
}

func (g *GroupByGenericWorker) processedCountMessage(message amqp.Delivery) error {
	msg, err := middleware.NewMessageProcessedFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	clientStats := g.getClientStats(msg.ClientId)

	clientStats.AddProcessed(msg.DataType)
	// no need to track emitted here, as this worker does not emit any message yet

	if prevEofEmittedCount, exists := clientStats.GetEof(msg.DataType); exists {
		// EOF ARRIVED!
		if clientStats.GetProcessed(msg.DataType) == prevEofEmittedCount {
			g.log.Infof("All messages processed for client %s and dataType %s", msg.ClientId, msg.DataType)
			clientStats.SendEofChan(msg.DataType)
		}
	}

	answerMessage(ACK, message)
	return nil
}

func (g *GroupByGenericWorker) gatherAndSendPartialResults(message amqp.Delivery) error {
	msg, err := middleware.NewMessageResultsRequestFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}
	g.log.Infof("Received request to gather and send partial results from %s to queue %s", msg.Origin, msg.QueueName)
	if msg.Origin == g.conf.id {
		g.log.Infof("Ignoring request to gather and send partial results from myself %s", msg.Origin)
		answerMessage(ACK, message)
		return nil
	}

	g.mutex.Lock()
	currentGroup := g.group.Get(msg.ClientId, g.conf.factory)
	g.mutex.Unlock()

	// IF WE WANT TO BATCH RESULTS, WE CAN DO IT HERE
	partialResults := currentGroup.ToMapString()
	messageToSend := middleware.NewMessageGrouped(msg.DataType, msg.ClientId, partialResults, false, 0)
	responseBytes, err := messageToSend.ToBytes()
	if err != nil {
		g.log.Errorf("%v", err)
		answerMessage(NACK_DISCARD, message)
		return err
	}
	g.log.Infof("Sending partial results to %s: %v", msg.QueueName, partialResults)

	// SEND TO REQUESTOR
	middleError := g.SendToQueue(msg.QueueName, responseBytes)
	if middleError != middleware.MessageMiddlewareSuccess {
		answerMessage(NACK_REQUEUE, message)
		return fmt.Errorf("problem while sending message to %s", msg.QueueName)
	}

	g.log.Infof("Partial results sent to %s", msg.QueueName)
	// DELETE AFTER SENDING
	// At this point, client has finished, results have been sent to the requester, so we can delete the stored group
	g.mutex.Lock()
	g.group.Delete(msg.ClientId)
	g.mutex.Unlock()
	g.log.Infof("Deleted stored group for client %s", msg.ClientId)

	answerMessage(ACK, message)
	return nil
}

func (g *GroupByGenericWorker) SendToQueue(queueName string, message []byte) middleware.MessageMiddlewareError {
	// declare queue many to one (many publishers one consumer)
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
	g.exchangeHandlers.broadcastCountSub.StartConsuming(g.processedCountMessage, g.errChan)

	// ACA VOY A SETEAR AL DE GATHER RESULTS
	g.exchangeHandlers.broadcastResultsRequestSub.StartConsuming(g.gatherAndSendPartialResults, g.errChan)

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
	g.exchangeHandlers.broadcastCountSub.StopConsuming()
	g.exchangeHandlers.broadcastCountSub.Close()
	g.exchangeHandlers.broadcastCountPub.Close()
	g.exchangeHandlers.broadcastResultsRequestSub.StopConsuming()
	g.exchangeHandlers.broadcastResultsRequestSub.Close()
	g.middlewareHandler.Close()

	g.log.Info("Shutdown complete")
}
