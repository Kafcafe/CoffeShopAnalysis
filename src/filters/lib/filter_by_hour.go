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
	transactionsYearFilteredSubscription middleware.MessageMiddlewareExchange
}

// handleSignal listens for SIGTERM signal and triggers shutdown.
func (f *FilterByHourWorker) handleSignal() {
	<-f.sigChan
	f.log.Info("Handling signal")
	f.Shutdown()
}

func NewFilterByHourWorker(rabbitConf middleware.RabbitConfig, filtersConfig HourFilterConfig) (*FilterByHourWorker, error) {
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

func (f *FilterByHourWorker) createExchangeHandlers() error {
	transactionsYearFilteredSubscriptionRouteKey := "transactions.transactions"
	transactionsYearFilteredSubscriptionHandler, err := createExchangeHandler(f.rabbitConn, transactionsYearFilteredSubscriptionRouteKey, middleware.EXCHANGE_TYPE_DIRECT)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for transactions: %v", err)
	}

	f.exchangeHandlers = HourExchangeHandlers{
		transactionsYearFilteredSubscription: *transactionsYearFilteredSubscriptionHandler,
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
	filteredBatch := filter.FilterByHour(batch, f.conf.FromHour, f.conf.ToHour)
	if len(filteredBatch) == 0 {
		f.log.Info("No transaction passed the filterMessageByHour")
		f.answerMessage(ACK, message)
		return nil
	}

	payload, err := json.Marshal(filteredBatch)
	if err != nil {
		return fmt.Errorf("problem while marshalling batch of dataType %s: %w", msg.DataType, err)
	}

	newMsg := middleware.NewMessage(msg.DataType, msg.ClientId, payload)
	_, err = json.Marshal(newMsg)
	if err != nil {
		return fmt.Errorf("problem while marshalling batch of dataType %s: %w", msg.DataType, err)
	}

	f.answerMessage(ACK, message)

	// switch msg.DataType {
	// case "transactions":
	// 	f.exchangeHandlers.transactionsYearFilteredPublishing.Send(msgBytes)
	// 	f.answerMessage(ACK, message)
	// case "transaction_items":
	// 	f.exchangeHandlers.transactionsItemsYearFilteredPublishing.Send(msgBytes)
	// 	f.answerMessage(ACK, message)
	// default:
	// 	return fmt.Errorf("received unprocessabble message in filterMessageByHour of type %s", msg.DataType)
	// }

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
