package group

import (
	"common/logger"
	"common/middleware"
	"fmt"
	"group/lib/structures"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type StoreExchangeHandlers struct {
	transactionsYearHourFilteredSubscription middleware.MessageMiddlewareExchange
	transactionsGroupedByStorePublishing     middleware.MessageMiddlewareExchange
	eofPublishing                            middleware.MessageMiddlewareExchange
	eofSubscription                          middleware.MessageMiddlewareQueue
}

type GroupByStoreWorker struct {
	log                       *logging.Logger
	rabbitConn                *middleware.RabbitConnection
	sigChan                   chan os.Signal
	isRunning                 bool
	exchangeHandlers          StoreExchangeHandlers
	errChan                   chan middleware.MessageMiddlewareError
	id                        string
	groupByCount              int
	currentMessageProcessing  middleware.Message
	mutex                     sync.Mutex
	eofChan                   chan int
	eofIntercommunicationChan chan structures.StoreGroup
	groupedPerClient          structures.StoreGroupPerClient
}

func NewGroupByStoreWorker(rabbitConf middleware.RabbitConfig, groupById string, groupByCount int) (*GroupByStoreWorker, error) {
	log := logger.GetLoggerWithPrefix("[GROUP-ST]")

	log.Infof("Establishing connection with RabbitMQ on address %s:%d", rabbitConf.Host, rabbitConf.Port)

	rabbitConn, err := middleware.NewRabbitConnection(&rabbitConf)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	log.Info("Connection with RabbitMQ successfully established")

	sigChan := make(chan os.Signal, SINGLE_ITEM_BUFFER_LEN)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	return &GroupByStoreWorker{
		log:                       log,
		rabbitConn:                rabbitConn,
		sigChan:                   sigChan,
		isRunning:                 true,
		errChan:                   make(chan middleware.MessageMiddlewareError, ERROR_CHANNEL_BUFFER_SIZE),
		id:                        groupById,
		groupByCount:              groupByCount,
		mutex:                     sync.Mutex{},
		eofChan:                   make(chan int, SINGLE_ITEM_BUFFER_LEN),
		eofIntercommunicationChan: make(chan structures.StoreGroup, SINGLE_ITEM_BUFFER_LEN),
		groupedPerClient:          structures.NewStoreGroupPerClient(),
	}, nil
}

// handleSignal listens for SIGTERM signal and triggers shutdown.
func (g *GroupByStoreWorker) handleSignal() {
	<-g.sigChan
	g.log.Info("Handling signal")
	g.Shutdown()
}

func (g *GroupByStoreWorker) processInboundEof(message amqp.Delivery) error {
	defer answerMessage(NACK_DISCARD, message)

	msg, err := middleware.NewEofMessageGroupedFromBytes(message.Body)
	if err != nil {
		return err
	}
	g.log.Warningf("processInboundEof %s groupBy%s", msg.DataType, g.id)

	didSomebodyElseAcked := msg.Origin == g.id && msg.IsAck && msg.ImmediateSource != g.id
	if didSomebodyElseAcked {
		partialGrouping := structures.NewStoreGroupFromMapString(msg.Payload)
		g.log.Infof("%v", partialGrouping)
		g.eofIntercommunicationChan <- partialGrouping
		return nil
	}

	isAckMine := msg.ImmediateSource == g.id
	isAckForNotForMe := msg.IsAck && msg.Origin != g.id
	if isAckMine || isAckForNotForMe {
		answerMessage(ACK, message)
		return nil
	}

	g.log.Warning("Lock")
	g.mutex.Lock()
	currentMessageProcessing := g.currentMessageProcessing
	g.mutex.Unlock()
	g.log.Warning("Unlock")

	if currentMessageProcessing.IsFromSameStream(msg.DataType, msg.ClientId) {
		g.log.Warningf("BEFORE INBOUND %s", msg.DataType)
		<-g.eofChan
		g.log.Warningf("AFTER INBOUND %s", msg.DataType)
	}

	msg.ImmediateSource = g.id
	msg.IsAck = true

	g.mutex.Lock()
	msg.Payload = g.groupedPerClient.Get(msg.ClientId).ToMapString()
	g.mutex.Unlock()

	msgBytes, err := msg.ToBytes()
	if err != nil {
		return err
	}

	answerMessage(ACK, message)
	g.exchangeHandlers.eofPublishing.Send(msgBytes)
	return nil
}

func (g *GroupByStoreWorker) initiateEofCoordination(originalMsg middleware.Message, originalMsgBytes []byte) {
	eofMsg := middleware.NewEofMessageGrouped(originalMsg.DataType, originalMsg.ClientId, g.id, g.id, false, nil)
	msgBytes, err := eofMsg.ToBytes()
	if err != nil {
		g.log.Errorf("Failed to serialize message: %v", err)
	}

	g.exchangeHandlers.eofPublishing.Send(msgBytes)

	totalEofs := g.groupByCount - 1

	if totalEofs == 0 {
		g.log.Infof("No EOF coordination needed for %s", originalMsg.DataType)
	} else {
		g.log.Infof("Coordinating EOF for %s", originalMsg.DataType)
	}

	g.log.Infof("Consolidating partial results for %s", originalMsg.DataType)

	g.mutex.Lock()
	clientSemesterGroup := g.groupedPerClient.Get(originalMsg.ClientId)
	g.groupedPerClient.Delete(originalMsg.ClientId)
	g.mutex.Unlock()

	for i := 0; i < totalEofs; i++ {
		g.log.Warningf("BEFORE %d %s", i, originalMsg.DataType)
		partialGrouping := <-g.eofIntercommunicationChan
		clientSemesterGroup.Merge(partialGrouping)
		g.log.Warningf("AFTER %d %s", i, originalMsg.DataType)
	}

	allGroupedByClient := clientSemesterGroup.ToMapString()

	for key, records := range allGroupedByClient {
		singleSemesterRecords := map[string][]string{key: records}
		response := middleware.NewMessageGrouped(originalMsg.DataType, originalMsg.ClientId, singleSemesterRecords, false)
		responseBytes, err := response.ToBytes()
		if err != nil {
			g.log.Errorf("%v", err)
		}

		g.log.Infof("Sent consolidated results for semester: %s", key)

		middleError := g.exchangeHandlers.transactionsGroupedByStorePublishing.Send(responseBytes)
		if middleError != middleware.MessageMiddlewareSuccess {
			g.log.Errorf("problem while sending message to resultsQ3Publishing")
		}
	}
	g.log.Infof("Final results grouped and consolidated")

	middleError := g.exchangeHandlers.transactionsGroupedByStorePublishing.Send(originalMsgBytes)
	if middleError != middleware.MessageMiddlewareSuccess {
		g.log.Errorf("problem while propagating EOF")
	}

	g.log.Warningf("Propagated EOF for %s to next pipeline stage", originalMsg.DataType)
}

func (g *GroupByStoreWorker) groupByStore(message amqp.Delivery) error {
	defer answerMessage(NACK_DISCARD, message)

	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		return err
	}

	if msg.IsEof {
		go g.initiateEofCoordination(*msg, message.Body)
		answerMessage(ACK, message)
		return nil
	}

	if len(g.eofChan) > 0 {
		<-g.eofChan
	}

	g.mutex.Lock()
	g.currentMessageProcessing = *msg
	g.currentMessageProcessing.Payload = []string{}

	g.groupedPerClient.Add(msg.ClientId, msg.Payload)
	g.mutex.Unlock()
	answerMessage(ACK, message)

	g.eofChan <- THERE_IS_PREVIOUS_MESSAGE

	g.log.Info("Grouped message and sent groupByStore batch")
	return nil
}

func (f *GroupByStoreWorker) createExchangeHandlers() error {
	transactionsYearHourFilteredSubscriptionRouteyKey := "transactions.transactions"
	transactionsYearHourFilteredSubscriptionHandler, err := createExchangeHandler(f.rabbitConn, transactionsYearHourFilteredSubscriptionRouteyKey, middleware.EXCHANGE_TYPE_DIRECT)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for transactions.transactions: %v", err)
	}

	transactionsGroupedByStorePublishingRouteKey := "transactions.items.group.store"
	transactionsGroupedByStorePublishingHandler, err := createExchangeHandler(f.rabbitConn, transactionsGroupedByStorePublishingRouteKey, middleware.EXCHANGE_TYPE_DIRECT)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for transactions.items.group.store: %v", err)
	}

	eofPublishingRouteKey := fmt.Sprintf("eof.group.store.%s", f.id)
	eofPublishingHandler, err := createExchangeHandler(f.rabbitConn, eofPublishingRouteKey, middleware.EXCHANGE_TYPE_TOPIC)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for eof.group.store: %v", err)
	}

	eofSubscription, err := prepareEofQueue(f.rabbitConn, "store", f.id)
	if err != nil {
		return fmt.Errorf("Error preparing EOF queue for transactions: %v", err)
	}

	f.exchangeHandlers = StoreExchangeHandlers{
		transactionsYearHourFilteredSubscription: *transactionsYearHourFilteredSubscriptionHandler,
		transactionsGroupedByStorePublishing:     *transactionsGroupedByStorePublishingHandler,
		eofPublishing:                            *eofPublishingHandler,
		eofSubscription:                          *eofSubscription,
	}

	return nil
}

func (g *GroupByStoreWorker) Run() error {
	defer g.Shutdown()
	go g.handleSignal()

	err := g.createExchangeHandlers()
	if err != nil {
		return fmt.Errorf("failed to create exchange handlers: %v", err)
	}

	g.exchangeHandlers.transactionsYearHourFilteredSubscription.StartConsuming(g.groupByStore, g.errChan)
	g.exchangeHandlers.eofSubscription.StartConsuming(g.processInboundEof, g.errChan)

	for err := range g.errChan {
		if err != middleware.MessageMiddlewareSuccess {
			g.log.Errorf("Error found while grouping by Semester message of type: %v", err)
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
func (g *GroupByStoreWorker) Shutdown() {
	g.isRunning = false
	g.errChan <- middleware.MessageMiddlewareSuccess

	g.exchangeHandlers.eofSubscription.StopConsuming()
	g.exchangeHandlers.eofSubscription.Close()
	g.exchangeHandlers.eofPublishing.Close()
	g.rabbitConn.Close()

	g.log.Info("Shutdown complete")
}
