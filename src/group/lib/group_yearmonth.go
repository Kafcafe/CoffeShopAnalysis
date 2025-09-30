package group

import (
	"common/logger"
	"common/middleware"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type YearmonthExchangeHandlers struct {
	transactionItemsYearFilteredSubscription     middleware.MessageMiddlewareExchange
	transactionItemsGroupedByYearmonthPublishing middleware.MessageMiddlewareExchange
	eofPublishing                                middleware.MessageMiddlewareExchange
	eofSubscription                              middleware.MessageMiddlewareQueue
}

type GroupByYearmonthWorker struct {
	log                       *logging.Logger
	rabbitConn                *middleware.RabbitConnection
	sigChan                   chan os.Signal
	isRunning                 bool
	exchangeHandlers          YearmonthExchangeHandlers
	errChan                   chan middleware.MessageMiddlewareError
	id                        string
	groupByCount              int
	currentMessageProcessing  middleware.Message
	mutex                     sync.Mutex
	eofChan                   chan int
	eofIntercommunicationChan chan int
}

func NewGroupByYearmonthWorker(rabbitConf middleware.RabbitConfig, groupById string, groupByCount int) (*GroupByYearmonthWorker, error) {
	log := logger.GetLoggerWithPrefix("[GROUP-YM]")

	log.Infof("Establishing connection with RabbitMQ on address %s:%d", rabbitConf.Host, rabbitConf.Port)

	rabbitConn, err := middleware.NewRabbitConnection(&rabbitConf)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	log.Info("Connection with RabbitMQ successfully established")

	sigChan := make(chan os.Signal, SINGLE_ITEM_BUFFER_LEN)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	return &GroupByYearmonthWorker{
		log:                       log,
		rabbitConn:                rabbitConn,
		sigChan:                   sigChan,
		isRunning:                 true,
		errChan:                   make(chan middleware.MessageMiddlewareError, ERROR_CHANNEL_BUFFER_SIZE),
		id:                        groupById,
		groupByCount:              groupByCount,
		mutex:                     sync.Mutex{},
		eofChan:                   make(chan int, SINGLE_ITEM_BUFFER_LEN),
		eofIntercommunicationChan: make(chan int, SINGLE_ITEM_BUFFER_LEN),
	}, nil
}

// handleSignal listens for SIGTERM signal and triggers shutdown.
func (g *GroupByYearmonthWorker) handleSignal() {
	<-g.sigChan
	g.log.Info("Handling signal")
	g.Shutdown()
}

// HERE WE NEED TO ADD THE LOGIC FOR THE PARTIAL RESULTS GROUPING!
func (g *GroupByYearmonthWorker) processInboundEof(message amqp.Delivery) error {
	defer answerMessage(NACK_DISCARD, message)

	msg, err := middleware.NewEofMessageFromBytes(message.Body)
	if err != nil {
		return err
	}
	g.log.Warningf("processInboundEof %s groupBy%s", msg.DataType, g.id)

	didSomebodyElseAcked := msg.Origin == g.id && msg.IsAck && msg.ImmediateSource != g.id
	if didSomebodyElseAcked {
		g.eofIntercommunicationChan <- ACTIVITY
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

	if currentMessageProcessing.IsFromSameStream(msg) {
		g.log.Warningf("BEFORE INBOUND %s", msg.DataType)
		<-g.eofChan
		g.log.Warningf("AFTER INBOUND %s", msg.DataType)
	}

	msg.ImmediateSource = g.id
	msg.IsAck = true
	msgBytes, err := msg.ToBytes()
	if err != nil {
		return err
	}

	answerMessage(ACK, message)
	g.exchangeHandlers.eofPublishing.Send(msgBytes)
	return nil
}

func (g *GroupByYearmonthWorker) initiateEofCoordination(originalMsg middleware.Message, originalMsgBytes []byte) {
	eofMsg := middleware.NewEofMessage(originalMsg.DataType, originalMsg.ClientId, g.id, g.id, false)
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

	for i := 0; i < totalEofs; i++ {
		g.log.Warningf("BEFORE %d %s", i, originalMsg.DataType)
		<-g.eofIntercommunicationChan
		g.log.Warningf("AFTER %d %s", i, originalMsg.DataType)
	}

	middleError := g.exchangeHandlers.transactionItemsGroupedByYearmonthPublishing.Send(originalMsgBytes)
	if middleError != middleware.MessageMiddlewareSuccess {
		g.log.Error("problem while sending message to resultsQ1Publishing")
	}

	g.log.Warningf("Propagated EOF for %s to next pipeline stage", originalMsg.DataType)
}

func (g *GroupByYearmonthWorker) groupByYearmonth(message amqp.Delivery) error {
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
	g.mutex.Unlock()

	// IMPLEMENT GROUPBY LOGIC FOR COMPUTING PARTIAL RESULTS
	// groupBy := NewGr()
	// filteredBatch := filter.FilterByAmount(msg.Payload, f.conf.MinAmount)
	// if len(filteredBatch) == 0 {
	// 	f.log.Info("No transaction passed the filterMessageByAmount")
	// 	answerMessage(ACK, message)
	// 	f.eofChan <- THERE_IS_PREVIOUS_MESSAGE
	// 	return nil
	// }

	// SENDING SAME PAYLOAD BECAUSE WE ARE NOT GROUPING YET
	response := middleware.NewMessage(msg.DataType, msg.ClientId, msg.Payload, false)
	responseBytes, err := response.ToBytes()
	if err != nil {
		return err
	}

	middleError := g.exchangeHandlers.transactionItemsGroupedByYearmonthPublishing.Send(responseBytes)
	if middleError != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("problem while sending message to transactionItemsGroupedByYearmonthPublishing")
	}
	answerMessage(ACK, message)

	g.eofChan <- THERE_IS_PREVIOUS_MESSAGE

	g.log.Info("Grouped message and sent groupByYearmonth batch")
	return nil
}

func (f *GroupByYearmonthWorker) createExchangeHandlers() error {
	transactionItemsYearFilteredSubscriptionRouteKey := "transactions.items"
	transactionItemsYearFilteredSubscriptionHandler, err := createExchangeHandler(f.rabbitConn, transactionItemsYearFilteredSubscriptionRouteKey, middleware.EXCHANGE_TYPE_DIRECT)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for transactions.items: %v", err)
	}

	transactionItemsGroupedByYearmonthPublishingRouteKey := fmt.Sprintf("transactions.items.group%s", f.id)
	transactionItemsGroupedByYearmonthPublishingHandler, err := createExchangeHandler(f.rabbitConn, transactionItemsGroupedByYearmonthPublishingRouteKey, middleware.EXCHANGE_TYPE_DIRECT)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for results.q1: %v", err)
	}

	eofPublishingRouteKey := fmt.Sprintf("eof.group.yearmonth.%s", f.id)
	eofPublishingHandler, err := createExchangeHandler(f.rabbitConn, eofPublishingRouteKey, middleware.EXCHANGE_TYPE_TOPIC)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for eof.group.yearmonth: %v", err)
	}

	eofSubscription, err := prepareEofQueue(f.rabbitConn, "yearmonth", f.id)
	if err != nil {
		return fmt.Errorf("Error preparing EOF queue for transactions: %v", err)
	}

	f.exchangeHandlers = YearmonthExchangeHandlers{
		transactionItemsYearFilteredSubscription:     *transactionItemsYearFilteredSubscriptionHandler,
		transactionItemsGroupedByYearmonthPublishing: *transactionItemsGroupedByYearmonthPublishingHandler,
		eofPublishing:   *eofPublishingHandler,
		eofSubscription: *eofSubscription,
	}

	return nil
}

func (g *GroupByYearmonthWorker) Run() error {
	defer g.Shutdown()
	go g.handleSignal()

	err := g.createExchangeHandlers()
	if err != nil {
		return fmt.Errorf("failed to create exchange handlers: %v", err)
	}

	g.exchangeHandlers.transactionItemsYearFilteredSubscription.StartConsuming(g.groupByYearmonth, g.errChan)
	g.exchangeHandlers.eofSubscription.StartConsuming(g.processInboundEof, g.errChan)

	for err := range g.errChan {
		if err != middleware.MessageMiddlewareSuccess {
			g.log.Errorf("Error found while grouping by Yearmonth message of type: %v", err)
		}

		if !g.isRunning {
			g.log.Info("Inside error loop: breaking")
			break
		}
	}

	g.exchangeHandlers.eofSubscription.StopConsuming()
	g.exchangeHandlers.eofSubscription.Close()
	g.exchangeHandlers.eofPublishing.Close()
	g.rabbitConn.Close()

	g.log.Info("Finished grouping")
	return nil
}

// Shutdown gracefully stops the acceptor, closing the listener and current client.
func (g *GroupByYearmonthWorker) Shutdown() {
	g.isRunning = false
	g.errChan <- middleware.MessageMiddlewareSuccess

	g.exchangeHandlers.eofSubscription.StopConsuming()
	g.exchangeHandlers.eofSubscription.Close()
	g.exchangeHandlers.eofPublishing.Close()
	g.rabbitConn.Close()

	g.log.Info("Shutdown complete")
}
