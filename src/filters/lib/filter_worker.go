package filters

import (
	"common/logger"
	"common/middleware"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type FilterWorker struct {
	log              *logging.Logger
	rabbitConn       *middleware.RabbitConnection
	sigChan          chan os.Signal
	isRunning        bool
	conf             FiltersConfig
	exchangeHandlers ExchangeHandlers
	errChan          chan middleware.MessageMiddlewareError
}

const (
	ERROR_CHANNEL_BUFFER_SIZE = 20
	SINGLE_ITEM_BUFFER_LEN    = 1
)

type ExchangeHandlers struct {
	// General
	transactionsSubscribing                 middleware.MessageMiddlewareExchange
	transactionsYearFilteredPublishing      middleware.MessageMiddlewareExchange
	transactionsItemsYearFilteredPublishing middleware.MessageMiddlewareExchange
}

// handleSignal listens for SIGTERM signal and triggers shutdown.
func (f *FilterWorker) handleSignal() {
	<-f.sigChan
	f.Shutdown()
}

func NewFilterWorker(rabbitConf middleware.RabbitConfig, filtersConfig FiltersConfig) (*FilterWorker, error) {
	log := logger.GetLoggerWithPrefix("[FILTER]")

	log.Infof("Establishing connection with RabbitMQ on address %s:%d", rabbitConf.Host, rabbitConf.Port)

	rabbitConn, err := middleware.NewRabbitConnection(&rabbitConf)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	log.Info("Connection with RabbitMQ successfully established")

	sigChan := make(chan os.Signal, SINGLE_ITEM_BUFFER_LEN)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	return &FilterWorker{
		log:        log,
		rabbitConn: rabbitConn,
		sigChan:    sigChan,
		isRunning:  true,
		conf:       filtersConfig,
		errChan:    make(chan middleware.MessageMiddlewareError, ERROR_CHANNEL_BUFFER_SIZE),
	}, nil
}

func (f *FilterWorker) createExchangeHandler(routeKey string, exchangeType string) (*middleware.MessageMiddlewareExchange, error) {
	middlewareHandler, err := middleware.NewMiddlewareHandler(f.rabbitConn)
	if err != nil {
		return nil, fmt.Errorf("fialed to create middleware handler: %w", err)
	}

	if exchangeType == middleware.EXCHANGE_TYPE_DIRECT {
		return middlewareHandler.CreateDirectExchange(routeKey)
	}
	return middlewareHandler.CreateTopicExchange(routeKey)
}

func (f *FilterWorker) createExchangeHandlers() error {
	transactionsRouteKey := fmt.Sprintf("transactions")
	transactionsSubscribingHandler, err := f.createExchangeHandler(transactionsRouteKey, middleware.EXCHANGE_TYPE_DIRECT)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for transactions: %v", err)
	}

	transactionsYearFilteredPublishingRouteKey := fmt.Sprintf("transactions.transactions")
	transactionsYearFilteredPublishingHandler, err := f.createExchangeHandler(transactionsYearFilteredPublishingRouteKey, middleware.EXCHANGE_TYPE_TOPIC)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for transactions.transactions: %v", err)
	}

	transactionsItemsYearFilteredPublishingRouteKey := fmt.Sprintf("transactions.items")
	transactionsItemsYearFilteredPublishingHandler, err := f.createExchangeHandler(transactionsItemsYearFilteredPublishingRouteKey, middleware.EXCHANGE_TYPE_TOPIC)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for transactions.items: %v", err)
	}

	f.exchangeHandlers = ExchangeHandlers{
		transactionsSubscribing:                 *transactionsSubscribingHandler,
		transactionsYearFilteredPublishing:      *transactionsYearFilteredPublishingHandler,
		transactionsItemsYearFilteredPublishing: *transactionsItemsYearFilteredPublishingHandler,
	}
	return nil
}

func (f *FilterWorker) filterMessageByYear(message amqp.Delivery) error {
	var msg middleware.Message
	err := json.Unmarshal(message.Body, &msg)
	if err != nil {
		return err
	}

	batch := []string{}
	err = json.Unmarshal(msg.Payload, &batch)
	if err != nil {
		return err
	}

	filter := NewFilter()
	filteredBatch := filter.FilterByYear(batch, f.conf.query1.FromYear, f.conf.query1.ToYear)
	if len(filteredBatch) == 0 {
		f.log.Info("No transaction passed the filterMessageByYear")
		return nil
	}

	payload, err := json.Marshal(filteredBatch)
	if err != nil {
		return fmt.Errorf("problem while marshalling batch of dataType %s: %w", msg.DataType, err)
	}

	newMsg := middleware.NewMessage(msg.DataType, msg.ClientId, payload)
	msgBytes, err := json.Marshal(newMsg)
	if err != nil {
		return fmt.Errorf("problem while marshalling batch of dataType %s: %w", msg.DataType, err)
	}

	switch msg.DataType {
	case "transaction":
		f.exchangeHandlers.transactionsYearFilteredPublishing.Send(msgBytes)
	case "transaction_items":
		f.exchangeHandlers.transactionsItemsYearFilteredPublishing.Send(msgBytes)
	default:
		return fmt.Errorf("received unprocessabble message in filterMessageByYear")
	}

	f.log.Info("Filtered message and sent filterMessageByYear batch")
	return nil
}

func (f *FilterWorker) Run() error {
	defer f.Shutdown()
	go f.handleSignal()

	err := f.createExchangeHandlers()
	if err != nil {
		return fmt.Errorf("failed to create exchange handlers: %v", err)
	}

	f.exchangeHandlers.transactionsSubscribing.StartConsuming(f.filterMessageByYear, f.errChan)

	for err := range <-f.errChan {
		if err != middleware.MessageMiddlewareSuccess {
			f.log.Errorf("Error found while filtering message of type: %v", err)
		}

		if !f.isRunning {
			break
		}
	}

	f.exchangeHandlers.transactionsSubscribing.Close()

	f.log.Info("Finished filtering")
	return nil
}

// Shutdown gracefully stops the acceptor, closing the listener and current client.
func (f *FilterWorker) Shutdown() {
	f.isRunning = false
	f.errChan <- middleware.MessageMiddlewareSuccess
	f.rabbitConn.Close()

	f.log.Info("Shutdown complete")
}
