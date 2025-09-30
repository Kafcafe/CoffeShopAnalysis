package filters

import (
	"common/logger"
	"common/middleware"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type FilterByAmountWorker struct {
	log              *logging.Logger
	rabbitConn       *middleware.RabbitConnection
	sigChan          chan os.Signal
	isRunning        bool
	conf             AmountFilterConfig
	exchangeHandlers AmountExchangeHandlers
	errChan          chan middleware.MessageMiddlewareError
}

type AmountExchangeHandlers struct {
	transactionsYearAndHourFilteredSubscription middleware.MessageMiddlewareExchange
	resultsQ1Publishing                         middleware.MessageMiddlewareExchange
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
		log:        log,
		rabbitConn: rabbitConn,
		sigChan:    sigChan,
		isRunning:  true,
		conf:       filtersConfig,
		errChan:    make(chan middleware.MessageMiddlewareError, ERROR_CHANNEL_BUFFER_SIZE),
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

	f.exchangeHandlers = AmountExchangeHandlers{
		transactionsYearAndHourFilteredSubscription: *transactionsYearAndHourFilteredSubscriptionHandler,
		resultsQ1Publishing:                         *resultsQ1PublishingHandler,
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

func (f *FilterByAmountWorker) filterMessageByAmount(message amqp.Delivery) error {
	defer f.answerMessage(NACK_DISCARD, message)

	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		return err
	}

	filter := NewFilter()
	filteredBatch := filter.FilterByAmount(msg.Payload, f.conf.MinAmount)
	if len(filteredBatch) == 0 {
		f.log.Info("No transaction passed the filterMessageByAmount")
		f.answerMessage(ACK, message)
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

	f.log.Info("Filtered message and sent filterMessageByAmount batch")
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

	f.log.Info("Finished filtering")
	return nil
}

// Shutdown gracefully stops the acceptor, closing the listener and current client.
func (f *FilterByAmountWorker) Shutdown() {
	f.isRunning = false
	f.errChan <- middleware.MessageMiddlewareSuccess
	f.rabbitConn.Close()

	f.log.Info("Shutdown complete")
}
