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

type FilterByHourWorker struct {
	log              *logging.Logger
	rabbitConn       *middleware.RabbitConnection
	sigChan          chan os.Signal
	isRunning        bool
	conf             HourFilterConfig
	exchangeHandlers HourExchangeHandlers
	errChan          chan middleware.MessageMiddlewareError
}

type HourExchangeHandlers struct {
	transactionsYearFilteredSubscription      middleware.MessageMiddlewareExchange
	transactionsYearAndHourFilteredPublishing middleware.MessageMiddlewareExchange
}

// handleSignal listens for SIGTERM signal and triggers shutdown.
func (f *FilterByHourWorker) handleSignal() {
	<-f.sigChan
	f.log.Info("Handling signal")
	f.Shutdown()
}

func NewFilterByHourWorker(rabbitConf middleware.RabbitConfig, filtersConfig HourFilterConfig, filterId string, filterCount int) (*FilterByHourWorker, error) {
	log := logger.GetLoggerWithPrefix("[FILTER-H]")

	log.Infof("Establishing connection with RabbitMQ on address %s:%d", rabbitConf.Host, rabbitConf.Port)

	rabbitConn, err := middleware.NewRabbitConnection(&rabbitConf)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	log.Info("Connection with RabbitMQ successfully established")

	sigChan := make(chan os.Signal, SINGLE_ITEM_BUFFER_LEN)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	return &FilterByHourWorker{
		log:        log,
		rabbitConn: rabbitConn,
		sigChan:    sigChan,
		isRunning:  true,
		conf:       filtersConfig,
		errChan:    make(chan middleware.MessageMiddlewareError, ERROR_CHANNEL_BUFFER_SIZE),
	}, nil
}

func (f *FilterByHourWorker) createExchangeHandlersForPostFiltering() (*middleware.MessageMiddlewareExchange, error) {
	middlewareHandler, err := middleware.NewMiddlewareHandler(f.rabbitConn)
	if err != nil {
		return nil, fmt.Errorf("fialed to create middleware handler: %w", err)
	}

	transactionsYearAndHourFilteredPublishingRouteKey := "transactions.year-hour-filtered.all"
	transactionsYearAndHourFilteredPublishingHandler, err := middlewareHandler.CreateTopicExchangeStandalone(transactionsYearAndHourFilteredPublishingRouteKey)
	if err != nil {
		return nil, fmt.Errorf("Error creating exchange handler for ransactions.year-hour-filtered.*: %v", err)
	}

	// Declare and bind for Query 1
	query1RouteKey := "transactions.year-hour-filtered.q1"
	_, err = middlewareHandler.DeclareQueue(query1RouteKey)
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue %s: %v", query1RouteKey, err)
	}

	err = middlewareHandler.BindQueue(query1RouteKey, middleware.EXCHANGE_NAME_TOPIC_TYPE, transactionsYearAndHourFilteredPublishingRouteKey)
	if err != nil {
		return nil, fmt.Errorf("failed to bind queue to exchange: %v", err)
	}

	// Declare and bind for Query 3
	query3RouteKey := "transactions.year-hour-filtered.q3"
	_, err = middlewareHandler.DeclareQueue(query3RouteKey)
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue %s: %v", query3RouteKey, err)
	}

	err = middlewareHandler.BindQueue(query3RouteKey, middleware.EXCHANGE_NAME_TOPIC_TYPE, transactionsYearAndHourFilteredPublishingRouteKey)
	if err != nil {
		return nil, fmt.Errorf("failed to bind queue to exchange: %v", err)
	}

	return transactionsYearAndHourFilteredPublishingHandler, nil
}

func (f *FilterByHourWorker) createExchangeHandlers() error {
	transactionsYearFilteredSubscriptionRouteKey := "transactions.transactions"
	transactionsYearFilteredSubscriptionHandler, err := createExchangeHandler(f.rabbitConn, transactionsYearFilteredSubscriptionRouteKey, middleware.EXCHANGE_TYPE_DIRECT)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for transactions: %v", err)
	}

	transactionsYearAndHourFilteredPublishingHandler, err := f.createExchangeHandlersForPostFiltering()
	if err != nil {
		return err
	}

	f.exchangeHandlers = HourExchangeHandlers{
		transactionsYearFilteredSubscription:      *transactionsYearFilteredSubscriptionHandler,
		transactionsYearAndHourFilteredPublishing: *transactionsYearAndHourFilteredPublishingHandler,
	}

	return nil
}

func (f *FilterByHourWorker) answerMessage(ackType int, message amqp.Delivery) {
	switch ackType {
	case ACK:
	case NACK_REQUEUE:
		message.Nack(false, true)
	case NACK_DISCARD:
		message.Nack(false, false)
	}
}

func (f *FilterByHourWorker) filterMessageByHour(message amqp.Delivery) error {
	defer f.answerMessage(NACK_DISCARD, message)

	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		return err
	}

	filter := NewFilter()
	filteredBatch := filter.FilterByHour(msg.Payload, f.conf.FromHour, f.conf.ToHour)
	if len(filteredBatch) == 0 {
		f.log.Info("No transaction passed the filterMessageByHour")
		f.answerMessage(ACK, message)
		return nil
	}

	response := middleware.NewMessage(msg.DataType, msg.ClientId, filteredBatch, false)
	responseBytes, err := response.ToBytes()
	if err != nil {
		return err
	}

	middleError := f.exchangeHandlers.transactionsYearAndHourFilteredPublishing.Send(responseBytes)
	if middleError != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("problem while sending message to transactionsYearAndHourFilteredPublishing")
	}
	f.answerMessage(ACK, message)

	f.log.Info("Filtered message and sent filterMessageByHour batch")
	return nil
}

func (f *FilterByHourWorker) Run() error {
	defer f.Shutdown()
	go f.handleSignal()

	err := f.createExchangeHandlers()
	if err != nil {
		return fmt.Errorf("failed to create exchange handlers: %v", err)
	}

	f.exchangeHandlers.transactionsYearFilteredSubscription.StartConsuming(f.filterMessageByHour, f.errChan)

	for err := range f.errChan {
		if err != middleware.MessageMiddlewareSuccess {
			f.log.Errorf("Error found while filtering message of type: %v", err)
		}

		if !f.isRunning {
			f.log.Info("Inside error loop: breaking")
			break
		}
	}

	f.exchangeHandlers.transactionsYearFilteredSubscription.Close()

	f.log.Info("Finished filtering")
	return nil
}

// Shutdown gracefully stops the acceptor, closing the listener and current client.
func (f *FilterByHourWorker) Shutdown() {
	f.isRunning = false
	f.errChan <- middleware.MessageMiddlewareSuccess
	f.rabbitConn.Close()

	f.log.Info("Shutdown complete")
}
