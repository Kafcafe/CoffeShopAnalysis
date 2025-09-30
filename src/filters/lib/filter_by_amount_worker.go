package filters

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

type FilterByAmountWorker struct {
	log                       *logging.Logger
	rabbitConn                *middleware.RabbitConnection
	sigChan                   chan os.Signal
	isRunning                 bool
	conf                      AmountFilterConfig
	exchangeHandlers          AmountExchangeHandlers
	errChan                   chan middleware.MessageMiddlewareError
	id                        string
	filtersCount              int
	currentMessageProcessing  middleware.Message
	mutex                     sync.Mutex
	eofChan                   chan int
	eofIntercommunicationChan chan int
}

type AmountExchangeHandlers struct {
	transactionsYearAndHourFilteredSubscription middleware.MessageMiddlewareExchange
	resultsQ1Publishing                         middleware.MessageMiddlewareExchange
	eofPublishing                               middleware.MessageMiddlewareExchange
	eofSubscription                             middleware.MessageMiddlewareQueue
}

// handleSignal listens for SIGTERM signal and triggers shutdown.
func (f *FilterByAmountWorker) handleSignal() {
	<-f.sigChan
	f.log.Info("Handling signal")
	f.Shutdown()
}

func NewFilterByAmountWorker(rabbitConf middleware.RabbitConfig, filtersConfig AmountFilterConfig, filterId string, filterCount int) (*FilterByAmountWorker, error) {
	log := logger.GetLoggerWithPrefix("[FILTER-A]")

	log.Infof("Establishing connection with RabbitMQ on address %s:%d", rabbitConf.Host, rabbitConf.Port)

	rabbitConn, err := middleware.NewRabbitConnection(&rabbitConf)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	log.Info("Connection with RabbitMQ successfully established")

	sigChan := make(chan os.Signal, SINGLE_ITEM_BUFFER_LEN)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	return &FilterByAmountWorker{
		log:                       log,
		rabbitConn:                rabbitConn,
		sigChan:                   sigChan,
		isRunning:                 true,
		conf:                      filtersConfig,
		errChan:                   make(chan middleware.MessageMiddlewareError, ERROR_CHANNEL_BUFFER_SIZE),
		id:                        filterId,
		filtersCount:              filterCount,
		mutex:                     sync.Mutex{},
		eofChan:                   make(chan int, SINGLE_ITEM_BUFFER_LEN),
		eofIntercommunicationChan: make(chan int, SINGLE_ITEM_BUFFER_LEN),
	}, nil
}

func (f *FilterByAmountWorker) createExchangeHandlers() error {
	transactionsYearAndHourFilteredSubscriptionRouteKey := "transactions.year-hour-filtered.q1"
	transactionsYearAndHourFilteredSubscriptionHandler, err := createExchangeHandler(f.rabbitConn, transactionsYearAndHourFilteredSubscriptionRouteKey, middleware.EXCHANGE_TYPE_DIRECT)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for transactions.year-hour-filtered.q1: %v", err)
	}

	prepareQuery1InputQueues(f.rabbitConn)
	prepareQuery1OutputQueues(f.rabbitConn)

	resultsQ1PublishingRouteKey := "results.q1"
	resultsQ1PublishingHandler, err := createExchangeHandler(f.rabbitConn, resultsQ1PublishingRouteKey, middleware.EXCHANGE_TYPE_DIRECT)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for results.q1: %v", err)
	}

	eofPublishingRouteKey := fmt.Sprintf("eof.filters.amount.%s", f.id)
	eofPublishingHandler, err := createExchangeHandler(f.rabbitConn, eofPublishingRouteKey, middleware.EXCHANGE_TYPE_TOPIC)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for eof.filters.amount: %v", err)
	}

	eofSubscription, err := prepareEofQueue(f.rabbitConn, "amount", f.id)
	if err != nil {
		return fmt.Errorf("Error preparing EOF queue for transactions: %v", err)
	}

	f.exchangeHandlers = AmountExchangeHandlers{
		transactionsYearAndHourFilteredSubscription: *transactionsYearAndHourFilteredSubscriptionHandler,
		resultsQ1Publishing:                         *resultsQ1PublishingHandler,
		eofPublishing:                               *eofPublishingHandler,
		eofSubscription:                             *eofSubscription,
	}

	return nil
}

func (f *FilterByAmountWorker) answerMessage(ackType int, message amqp.Delivery) {
	switch ackType {
	case ACK:
	case NACK_REQUEUE:
		message.Nack(false, true)
	case NACK_DISCARD:
		message.Nack(false, false)
	}
}

func (f *FilterByAmountWorker) initiateEofCoordination(originalMsg middleware.Message, originalMsgBytes []byte) {
	eofMsg := middleware.NewEofMessage(originalMsg.DataType, originalMsg.ClientId, f.id, f.id, false)
	msgBytes, err := eofMsg.ToBytes()
	if err != nil {
		f.log.Errorf("Failed to serialize message: %v", err)
	}

	f.exchangeHandlers.eofPublishing.Send(msgBytes)

	totalEofs := f.filtersCount - 1

	if totalEofs == 0 {
		f.log.Infof("No EOF coordination needed for %s", originalMsg.DataType)
	} else {
		f.log.Infof("Coordinating EOF for %s", originalMsg.DataType)
	}

	for i := 0; i < totalEofs; i++ {
		f.log.Warningf("BEFORE %d %s", i, originalMsg.DataType)
		<-f.eofIntercommunicationChan
		f.log.Warningf("AFTER %d %s", i, originalMsg.DataType)
	}

	middleError := f.exchangeHandlers.resultsQ1Publishing.Send(originalMsgBytes)
	if middleError != middleware.MessageMiddlewareSuccess {
		f.log.Error("problem while sending message to resultsQ1Publishing")
	}

	f.log.Warningf("Propagated EOF for %s to next pipeline stage", originalMsg.DataType)
}

func (f *FilterByAmountWorker) filterMessageByAmount(message amqp.Delivery) error {
	defer f.answerMessage(NACK_DISCARD, message)

	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		return err
	}

	if msg.IsEof {
		go f.initiateEofCoordination(*msg, message.Body)
		f.answerMessage(ACK, message)
		return nil
	}

	if len(f.eofChan) > 0 {
		<-f.eofChan
	}

	f.mutex.Lock()
	f.currentMessageProcessing = *msg
	f.currentMessageProcessing.Payload = []string{}
	f.mutex.Unlock()

	filter := NewFilter()
	filteredBatch := filter.FilterByAmount(msg.Payload, f.conf.MinAmount)
	if len(filteredBatch) == 0 {
		f.log.Info("No transaction passed the filterMessageByAmount")
		f.answerMessage(ACK, message)
		f.eofChan <- THERE_IS_PREVIOUS_MESSAGE
		return nil
	}

	response := middleware.NewMessage(msg.DataType, msg.ClientId, filteredBatch, false)
	responseBytes, err := response.ToBytes()
	if err != nil {
		return err
	}

	middleError := f.exchangeHandlers.resultsQ1Publishing.Send(responseBytes)
	if middleError != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("problem while sending message to resultsQ1Publishing")
	}
	f.answerMessage(ACK, message)

	f.eofChan <- THERE_IS_PREVIOUS_MESSAGE

	f.log.Info("Filtered message and sent filterMessageByAmount batch")
	return nil
}

func (f *FilterByAmountWorker) processInboundEof(message amqp.Delivery) error {
	defer f.answerMessage(NACK_DISCARD, message)

	msg, err := middleware.NewEofMessageFromBytes(message.Body)
	if err != nil {
		return err
	}
	f.log.Warningf("processInboundEof %s filter%s", msg.DataType, f.id)

	didSomebodyElseAcked := msg.Origin == f.id && msg.IsAck && msg.ImmediateSource != f.id
	if didSomebodyElseAcked {
		f.eofIntercommunicationChan <- ACTIVITY
		return nil
	}

	isAckMine := msg.ImmediateSource == f.id
	isAckForNotForMe := msg.IsAck && msg.Origin != f.id
	if isAckMine || isAckForNotForMe {
		f.answerMessage(ACK, message)
		return nil
	}

	f.log.Warning("Lock")
	f.mutex.Lock()
	currentMessageProcessing := f.currentMessageProcessing
	f.mutex.Unlock()
	f.log.Warning("Unlock")

	if currentMessageProcessing.IsFromSameStream(msg) {
		f.log.Warningf("BEFORE INBOUND %s", msg.DataType)
		<-f.eofChan
		f.log.Warningf("AFTER INBOUND %s", msg.DataType)
	}

	msg.ImmediateSource = f.id
	msg.IsAck = true
	msgBytes, err := msg.ToBytes()
	if err != nil {
		return err
	}

	f.answerMessage(ACK, message)
	f.exchangeHandlers.eofPublishing.Send(msgBytes)
	return nil
}

func (f *FilterByAmountWorker) Run() error {
	defer f.Shutdown()
	go f.handleSignal()

	err := f.createExchangeHandlers()
	if err != nil {
		return fmt.Errorf("failed to create exchange handlers: %v", err)
	}

	f.exchangeHandlers.transactionsYearAndHourFilteredSubscription.StartConsuming(f.filterMessageByAmount, f.errChan)
	f.exchangeHandlers.eofSubscription.StartConsuming(f.processInboundEof, f.errChan)

	for err := range f.errChan {
		if err != middleware.MessageMiddlewareSuccess {
			f.log.Errorf("Error found while filtering message of type: %v", err)
		}

		if !f.isRunning {
			f.log.Info("Inside error loop: breaking")
			break
		}
	}

	f.exchangeHandlers.transactionsYearAndHourFilteredSubscription.Close()
	f.exchangeHandlers.eofSubscription.StopConsuming()
	f.exchangeHandlers.eofSubscription.Close()
	f.exchangeHandlers.eofPublishing.Close()

	f.log.Info("Finished filtering")
	return nil
}

// Shutdown gracefully stops the acceptor, closing the listener and current client.
func (f *FilterByAmountWorker) Shutdown() {
	f.isRunning = false
	f.errChan <- middleware.MessageMiddlewareSuccess
	f.exchangeHandlers.transactionsYearAndHourFilteredSubscription.Close()
	f.exchangeHandlers.eofSubscription.StopConsuming()
	f.exchangeHandlers.eofSubscription.Close()
	f.exchangeHandlers.eofPublishing.Close()
	f.rabbitConn.Close()

	f.log.Info("Shutdown complete")
}
