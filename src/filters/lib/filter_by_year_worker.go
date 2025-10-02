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

type FilterByYearWorker struct {
	log                       *logging.Logger
	rabbitConn                *middleware.RabbitConnection
	sigChan                   chan os.Signal
	isRunning                 bool
	conf                      YearFilterConfig
	exchangeHandlers          YearExchangeHandlers
	errChan                   chan middleware.MessageMiddlewareError
	id                        string
	filtersCount              int
	currentMessageProcessing  middleware.Message
	mutex                     sync.Mutex
	eofChan                   chan int
	eofIntercommunicationChan chan int
}

type YearExchangeHandlers struct {
	transactionsSubscribing                 middleware.MessageMiddlewareExchange
	transactionsYearFilteredPublishing      middleware.MessageMiddlewareExchange
	transactionsItemsYearFilteredPublishing middleware.MessageMiddlewareExchange
	eofPublishing                           middleware.MessageMiddlewareExchange
	eofSubscription                         middleware.MessageMiddlewareQueue
}

// handleSignal listens for SIGTERM signal and triggers shutdown.
func (f *FilterByYearWorker) handleSignal() {
	<-f.sigChan
	f.log.Info("Handling signal")
	f.Shutdown()
}

func NewFilterByYearWorker(rabbitConf middleware.RabbitConfig, filtersConfig YearFilterConfig, filterId string, filterCount int) (*FilterByYearWorker, error) {
	log := logger.GetLoggerWithPrefix("[FILTER-Y]")

	log.Infof("Establishing connection with RabbitMQ on address %s:%d", rabbitConf.Host, rabbitConf.Port)

	rabbitConn, err := middleware.NewRabbitConnection(&rabbitConf)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	log.Info("Connection with RabbitMQ successfully established")

	sigChan := make(chan os.Signal, SINGLE_ITEM_BUFFER_LEN)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	return &FilterByYearWorker{
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

func (f *FilterByYearWorker) createExchangeHandlers() error {
	transactionsRouteKey := "transactions"
	transactionsSubscribingHandler, err := createExchangeHandler(f.rabbitConn, transactionsRouteKey, middleware.EXCHANGE_TYPE_DIRECT)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for transactions: %v", err)
	}

	middlewareHandler, err := middleware.NewMiddlewareHandler(f.rabbitConn)
	if err != nil {
		return fmt.Errorf("fialed to create middleware handler: %w", err)
	}

	transactionsYearFilteredPublishingRouteKey := "transactions.transactions.all"
	transactionsYearFilteredPublishingHandler, err := middlewareHandler.CreateTopicExchangeStandalone(transactionsYearFilteredPublishingRouteKey)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for transactions.transactions: %v", err)
	}

	transactionsItemsYearFilteredPublishingRouteKey := "transactions.items"
	transactionsItemsYearFilteredPublishingHandler, err := createExchangeHandler(f.rabbitConn, transactionsItemsYearFilteredPublishingRouteKey, middleware.EXCHANGE_TYPE_TOPIC)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for transactions.items: %v", err)
	}

	eofPublishingRouteKey := fmt.Sprintf("eof.filters.year.%s", f.id)
	eofPublishingHandler, err := createExchangeHandler(f.rabbitConn, eofPublishingRouteKey, middleware.EXCHANGE_TYPE_TOPIC)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for eof.filters.year: %v", err)
	}

	eofSubscription, err := prepareEofQueue(f.rabbitConn, "year", f.id)
	if err != nil {
		return fmt.Errorf("Error preparing EOF queue for transactions: %v", err)
	}

	f.exchangeHandlers = YearExchangeHandlers{
		transactionsSubscribing:                 *transactionsSubscribingHandler,
		transactionsYearFilteredPublishing:      *transactionsYearFilteredPublishingHandler,
		transactionsItemsYearFilteredPublishing: *transactionsItemsYearFilteredPublishingHandler,
		eofPublishing:                           *eofPublishingHandler,
		eofSubscription:                         *eofSubscription,
	}
	return nil
}

func (f *FilterByYearWorker) initiateEofCoordination(originalMsg middleware.Message, originalMsgBytes []byte) {
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

	switch originalMsg.DataType {
	case "transactions":
		f.exchangeHandlers.transactionsYearFilteredPublishing.Send(originalMsgBytes)
	case "transaction_items":
		f.exchangeHandlers.transactionsItemsYearFilteredPublishing.Send(originalMsgBytes)
	default:
		f.log.Errorf("Unknown dataType '%s' on initiateEofCoordination", originalMsg.DataType)
		return
	}

	f.log.Warningf("Propagated EOF for %s to next pipeline stage", originalMsg.DataType)
}

func (f *FilterByYearWorker) filterMessageByYear(message amqp.Delivery) error {
	defer answerMessage(NACK_DISCARD, message)

	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		return err
	}

	if msg.IsEof {
		go f.initiateEofCoordination(*msg, message.Body)
		answerMessage(ACK, message)
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
	filteredBatch := filter.FilterByYear(msg.Payload, f.conf.FromYear, f.conf.ToYear)
	if len(filteredBatch) == 0 {
		f.log.Info("No transaction passed the filterMessageByYear for")
		answerMessage(ACK, message)
		f.eofChan <- THERE_IS_PREVIOUS_MESSAGE
		return nil
	}

	isEof := false
	response := middleware.NewMessage(msg.DataType, msg.ClientId, filteredBatch, isEof)
	responseBytes, err := response.ToBytes()
	if err != nil {
		return err
	}

	switch msg.DataType {
	case "transactions":
		f.exchangeHandlers.transactionsYearFilteredPublishing.Send(responseBytes)
		answerMessage(ACK, message)
	case "transaction_items":
		f.exchangeHandlers.transactionsItemsYearFilteredPublishing.Send(responseBytes)
		answerMessage(ACK, message)
	default:
		f.eofChan <- THERE_IS_PREVIOUS_MESSAGE
		return fmt.Errorf("received unprocessabble message in filterMessageByYear of type %s", msg.DataType)
	}

	f.eofChan <- THERE_IS_PREVIOUS_MESSAGE

	f.log.Infof("Filtered message and sent filterMessageByYear batch")
	return nil
}

func (f *FilterByYearWorker) processInboundEof(message amqp.Delivery) error {
	defer answerMessage(NACK_DISCARD, message)

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
		answerMessage(ACK, message)
		return nil
	}

	f.log.Warning("Lock")
	f.mutex.Lock()
	currentMessageProcessing := f.currentMessageProcessing
	f.mutex.Unlock()
	f.log.Warning("Unlock")

	if currentMessageProcessing.IsFromSameStream(msg.DataType, msg.ClientId) {
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

	answerMessage(ACK, message)
	f.exchangeHandlers.eofPublishing.Send(msgBytes)
	return nil
}

func (f *FilterByYearWorker) Run() error {
	defer f.Shutdown()
	go f.handleSignal()

	err := f.createExchangeHandlers()
	if err != nil {
		return fmt.Errorf("failed to create exchange handlers: %v", err)
	}

	f.exchangeHandlers.transactionsSubscribing.StartConsuming(f.filterMessageByYear, f.errChan)
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

	f.exchangeHandlers.transactionsSubscribing.Close()
	f.exchangeHandlers.eofSubscription.StopConsuming()
	f.exchangeHandlers.eofSubscription.Close()
	f.exchangeHandlers.eofPublishing.Close()

	f.log.Info("Finished filtering")
	return nil
}

// Shutdown gracefully stops the acceptor, closing the listener and current client.
func (f *FilterByYearWorker) Shutdown() {
	f.isRunning = false
	f.errChan <- middleware.MessageMiddlewareSuccess
	f.rabbitConn.Close()

	f.exchangeHandlers.transactionsSubscribing.Close()
	f.exchangeHandlers.eofSubscription.StopConsuming()
	f.exchangeHandlers.eofSubscription.Close()
	f.exchangeHandlers.eofPublishing.Close()

	f.log.Info("Shutdown complete")
}
