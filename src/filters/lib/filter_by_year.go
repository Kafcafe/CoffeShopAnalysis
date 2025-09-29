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

type FilterByYearWorker struct {
	log              *logging.Logger
	rabbitConn       *middleware.RabbitConnection
	sigChan          chan os.Signal
	isRunning        bool
	conf             YearFilterConfig
	exchangeHandlers YearExchangeHandlers
	errChan          chan middleware.MessageMiddlewareError
}

type YearExchangeHandlers struct {
	transactionsSubscribing                 middleware.MessageMiddlewareExchange
	transactionsYearFilteredPublishing      middleware.MessageMiddlewareExchange
	transactionsItemsYearFilteredPublishing middleware.MessageMiddlewareExchange
}

// handleSignal listens for SIGTERM signal and triggers shutdown.
func (f *FilterByYearWorker) handleSignal() {
	<-f.sigChan
	f.log.Info("Handling signal")
	f.Shutdown()
}

func NewFilterByYearWorker(rabbitConf middleware.RabbitConfig, filtersConfig YearFilterConfig) (*FilterByYearWorker, error) {
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
		log:        log,
		rabbitConn: rabbitConn,
		sigChan:    sigChan,
		isRunning:  true,
		conf:       filtersConfig,
		errChan:    make(chan middleware.MessageMiddlewareError, ERROR_CHANNEL_BUFFER_SIZE),
	}, nil
}

func (f *FilterByYearWorker) createExchangeHandlers() error {
	transactionsRouteKey := "transactions"
	transactionsSubscribingHandler, err := createExchangeHandler(f.rabbitConn, transactionsRouteKey, middleware.EXCHANGE_TYPE_DIRECT)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for transactions: %v", err)
	}

	transactionsYearFilteredPublishingRouteKey := "transactions.transactions"
	transactionsYearFilteredPublishingHandler, err := createExchangeHandler(f.rabbitConn, transactionsYearFilteredPublishingRouteKey, middleware.EXCHANGE_TYPE_TOPIC)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for transactions.transactions: %v", err)
	}

	transactionsItemsYearFilteredPublishingRouteKey := "transactions.items"
	transactionsItemsYearFilteredPublishingHandler, err := createExchangeHandler(f.rabbitConn, transactionsItemsYearFilteredPublishingRouteKey, middleware.EXCHANGE_TYPE_TOPIC)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for transactions.items: %v", err)
	}

	f.exchangeHandlers = YearExchangeHandlers{
		transactionsSubscribing:                 *transactionsSubscribingHandler,
		transactionsYearFilteredPublishing:      *transactionsYearFilteredPublishingHandler,
		transactionsItemsYearFilteredPublishing: *transactionsItemsYearFilteredPublishingHandler,
	}
	return nil
}

func (f *FilterByYearWorker) answerMessage(ackType int, message amqp.Delivery) {
	switch ackType {
	case ACK:
	case NACK_REQUEUE:
		message.Nack(false, true)
	case NACK_DISCARD:
		message.Nack(false, false)
	}
}

func (f *FilterByYearWorker) filterMessageByYear(message amqp.Delivery) error {
	defer f.answerMessage(NACK_DISCARD, message)

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
	filteredBatch := filter.FilterByYear(batch, f.conf.FromYear, f.conf.ToYear)
	if len(filteredBatch) == 0 {
		f.log.Info("No transaction passed the filterMessageByYear")
		f.answerMessage(ACK, message)
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
	case "transactions":
		f.exchangeHandlers.transactionsYearFilteredPublishing.Send(msgBytes)
		f.answerMessage(ACK, message)
	case "transaction_items":
		f.exchangeHandlers.transactionsItemsYearFilteredPublishing.Send(msgBytes)
		f.answerMessage(ACK, message)
	default:
		return fmt.Errorf("received unprocessabble message in filterMessageByYear of type %s", msg.DataType)
	}

	f.log.Info("Filtered message and sent filterMessageByYear batch")
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

	f.log.Info("Finished filtering")
	return nil
}

// Shutdown gracefully stops the acceptor, closing the listener and current client.
func (f *FilterByYearWorker) Shutdown() {
	f.isRunning = false
	f.errChan <- middleware.MessageMiddlewareSuccess
	f.rabbitConn.Close()

	f.log.Info("Shutdown complete")
}
