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

func (f *GroupByYearmonthWorker) groupByYearmonth(message amqp.Delivery) error {
	return nil
}

func (f *GroupByYearmonthWorker) createExchangeHandlers() error {
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
