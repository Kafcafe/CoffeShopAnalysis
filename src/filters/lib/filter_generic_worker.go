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

type ClientId = string
type DataType = string

type FilterGenericWorker struct {
	log               *logging.Logger
	middlewareHandler *middleware.MiddlewareHandler
	sigChan           chan os.Signal
	isRunning         bool

	filter             Filter
	conf               FilterConfig
	middlewareHandlers MiddlewareHandlers
	errChan            chan middleware.MessageMiddlewareError
	// new eof
	clientsStatsMutex sync.Mutex
	clientsStats      map[ClientId]*middleware.ClientStats
}

type MiddlewareHandlers struct {
	prevStageSub      middleware.MessageMiddlewareQueue // consider a queue
	nextStagePubs     map[string]middleware.MessageMiddlewareExchange
	broadcastCountPub middleware.MessageMiddlewareExchange
	broadcastCountSub middleware.MessageMiddlewareQueue
}

// handleSignal listens for SIGTERM signal and triggers shutdown.
func (f *FilterGenericWorker) handleSignal() {
	<-f.sigChan
	f.log.Info("Handling signal")
	f.Shutdown()
}

func NewFilterGenericWorker(rabbitConf middleware.RabbitConfig, config FilterConfig) (*FilterGenericWorker, error) {
	log := logger.GetLoggerWithPrefix("[FILTER" + config.id + "] ")

	log.Infof("Establishing connection with RabbitMQ on address %s:%d", rabbitConf.Host, rabbitConf.Port)

	rabbitConn, err := middleware.NewRabbitConnection(&rabbitConf)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	middlewareHandler, err := middleware.NewMiddlewareHandler(rabbitConn)
	if err != nil {
		return nil, fmt.Errorf("failed to create middleware handler: %v", err)
	}

	log.Info("Connection with RabbitMQ successfully established")

	sigChan := make(chan os.Signal, SINGLE_ITEM_BUFFER_LEN)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	return &FilterGenericWorker{
		log:               log,
		middlewareHandler: middlewareHandler,
		sigChan:           sigChan,
		isRunning:         true,
		filter:            *NewFilter(),
		conf:              config,
		errChan:           make(chan middleware.MessageMiddlewareError, ERROR_CHANNEL_BUFFER_SIZE),
		clientsStatsMutex: sync.Mutex{},
		clientsStats:      make(map[ClientId]*middleware.ClientStats),
	}, nil
}

func (f *FilterGenericWorker) createExchangeHandlers() error {
	_, err := f.middlewareHandler.CreateDirectExchangeStandalone(f.conf.prevStageSub)
	if err != nil {
		return fmt.Errorf("error creating exchange handler for %s: %v", f.conf.prevStageSub, err)
	}
	// this name is just for identification purposes
	prevStageSubQueueName := f.conf.prevStageSub + "." + f.conf.ofType
	prevStageSub, err := f.middlewareHandler.CreateQueue(prevStageSubQueueName)
	if err != nil {
		return fmt.Errorf("error creating queue handler for %s: %v", prevStageSubQueueName, err)
	}

	err = f.middlewareHandler.BindQueue(prevStageSubQueueName, middleware.EXCHANGE_NAME_DIRECT_TYPE, f.conf.prevStageSub)
	if err != nil {
		return fmt.Errorf("error preparing queue for transactions: %v", err)
	}

	// Prepare next stage publishing handlers

	nextStagePubs := make(map[string]middleware.MessageMiddlewareExchange)
	for datatype, routeKey := range f.conf.nextStagePubs {
		f.log.Infof("Next stage publishing for datatype %s on routeKey %s", datatype, routeKey)
		exchange, err := f.middlewareHandler.CreateDirectExchangeStandalone(routeKey)
		if err != nil {
			return fmt.Errorf("error creating exchange handler for %s: %v", routeKey, err)
		}
		nextStagePubs[datatype] = *exchange
	}

	f.log.Infof("Setting up count PUB for filter %s", f.conf.id)
	broadcastCountPubRoutKey := fmt.Sprintf("filters.%s.count", f.conf.ofType)
	broadcastCountPub, err := f.middlewareHandler.CreateFanoutExchangeStandalone(broadcastCountPubRoutKey)
	if err != nil {
		return fmt.Errorf("error creating exchange handler for %s: %v", broadcastCountPubRoutKey, err)
	}

	f.log.Infof("Setting up count SUB for filter %s", f.conf.id)
	broadcastCountSubQueueName := fmt.Sprintf("filters.%s.count.%s", f.conf.ofType, f.conf.id)
	broadcastCountSub, err := f.middlewareHandler.CreateQueue(broadcastCountSubQueueName)
	if err != nil {
		return fmt.Errorf("error creating count queue for %s: %v", broadcastCountSubQueueName, err)
	}
	err = f.middlewareHandler.BindQueue(broadcastCountSubQueueName, broadcastCountPubRoutKey, "")

	if err != nil {
		return fmt.Errorf("error preparing count queue for %s: %v", f.conf.ofType, err)
	}

	f.middlewareHandlers = MiddlewareHandlers{
		prevStageSub:      *prevStageSub,
		nextStagePubs:     nextStagePubs,
		broadcastCountPub: *broadcastCountPub,
		broadcastCountSub: *broadcastCountSub,
	}
	return nil
}

func (f *FilterGenericWorker) createExchangeHandlersForFinalStage() error {
	_, err := f.middlewareHandler.CreateDirectExchangeStandalone(f.conf.prevStageSub)
	if err != nil {
		return fmt.Errorf("error creating exchange handler for %s: %v", f.conf.prevStageSub, err)
	}
	// this name is just for identification purposes
	prevStageSubQueueName := f.conf.prevStageSub + "." + f.conf.ofType
	prevStageSub, err := f.middlewareHandler.CreateQueue(prevStageSubQueueName)
	if err != nil {
		return fmt.Errorf("error creating queue handler for %s: %v", prevStageSubQueueName, err)
	}

	err = f.middlewareHandler.BindQueue(prevStageSubQueueName, middleware.EXCHANGE_NAME_DIRECT_TYPE, f.conf.prevStageSub)
	if err != nil {
		return fmt.Errorf("error preparing queue for transactions: %v", err)
	}

	// Prepare next stage publishing handlers

	f.log.Infof("Setting up count PUB for filter %s", f.conf.id)
	broadcastCountPubRoutKey := fmt.Sprintf("filters.%s.count", f.conf.ofType)
	broadcastCountPub, err := f.middlewareHandler.CreateFanoutExchangeStandalone(broadcastCountPubRoutKey)
	if err != nil {
		return fmt.Errorf("error creating exchange handler for %s: %v", broadcastCountPubRoutKey, err)
	}

	f.log.Infof("Setting up count SUB for filter %s", f.conf.id)
	broadcastCountSubQueueName := fmt.Sprintf("filters.%s.count.%s", f.conf.ofType, f.conf.id)
	broadcastCountSub, err := f.middlewareHandler.CreateQueue(broadcastCountSubQueueName)
	if err != nil {
		return fmt.Errorf("error creating count queue for %s: %v", broadcastCountSubQueueName, err)
	}
	err = f.middlewareHandler.BindQueue(broadcastCountSubQueueName, broadcastCountPubRoutKey, "")

	if err != nil {
		return fmt.Errorf("error preparing count queue for %s: %v", f.conf.ofType, err)
	}

	f.middlewareHandlers = MiddlewareHandlers{
		prevStageSub:      *prevStageSub,
		nextStagePubs:     make(map[string]middleware.MessageMiddlewareExchange),
		broadcastCountPub: *broadcastCountPub,
		broadcastCountSub: *broadcastCountSub,
	}
	return nil
}

func (f *FilterGenericWorker) sendProcessedMessage(msgProcessed *middleware.MessageProcessed) error {
	msgProcessedBytes, err := msgProcessed.ToBytes()
	if err != nil {
		return err
	}
	sendErr := f.middlewareHandlers.broadcastCountPub.Send(msgProcessedBytes)
	if sendErr != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to send processed count message: %v", sendErr)
	}
	return nil
}

func (f *FilterGenericWorker) filterMessage(message amqp.Delivery) error {
	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	if f.conf.ofType == FILTER_TYPE_AMOUNT {
		msg.QueryId = 1
	}

	if msg.IsEof {
		f.clientsStatsMutex.Lock()
		clientStats := f.getClientStats(msg.ClientId)
		clientStats.SetEof(msg.DataType, msg.TotalEmitted)
		f.clientsStatsMutex.Unlock()
		go f.handleEofMessage(message, *msg)
		return nil
	}

	filteredBatch := f.conf.messageCallback(&f.filter, msg.Payload)

	msgProcessed := middleware.NewMessageProcessed(msg.DataType, msg.ClientId, len(filteredBatch) > 0, msg.QueryId)
	err = f.sendProcessedMessage(msgProcessed)
	if err != nil {
		f.log.Errorf("Failed to send processed count message: %v", err)
		answerMessage(NACK_REQUEUE, message)
		return err
	}

	if len(filteredBatch) == 0 {
		f.log.Info("No transaction passed the filterMessage of type " + f.conf.ofType)
		answerMessage(ACK, message)
		return nil
	}

	response := middleware.NewMessage(msg.DataType, msg.ClientId, filteredBatch, false, msg.QueryId)
	err = f.sendNextStage(*response)
	if err != nil {
		f.log.Errorf("Failed to send message to next stage: %v", err)
		answerMessage(NACK_DISCARD, message)
		return err
	}

	answerMessage(ACK, message)
	f.log.Infof("Filtered message and sent filterMessage batch")
	return nil
}

func (f *FilterGenericWorker) sendNextStage(msgToSend middleware.Message) error {
	var nextStagePub middleware.MessageMiddlewareExchange
	var exists bool

	if f.conf.ofType == FILTER_TYPE_AMOUNT {
		nextStagePub, exists = f.middlewareHandlers.nextStagePubs[msgToSend.ClientId]
		if !exists {
			routeKey := fmt.Sprintf("results.%s", msgToSend.ClientId)
			f.log.Infof("Next stage publishing for datatype %s on routeKey %s", msgToSend.DataType, routeKey)
			exchange, err := f.middlewareHandler.CreateDirectExchangeStandalone(routeKey)
			if err != nil {
				return fmt.Errorf("error creating exchange handler for %s: %v", routeKey, err)
			}
			f.middlewareHandlers.nextStagePubs[msgToSend.ClientId] = *exchange
			nextStagePub = *exchange
		}
	} else {
		nextStagePub, exists = f.middlewareHandlers.nextStagePubs[msgToSend.DataType]
		if !exists {
			return fmt.Errorf("received unprocessabble message in sendNextStage of type %s", msgToSend.DataType)
		}
	}

	msgBytes, err := msgToSend.ToBytes()
	if err != nil {
		return err
	}
	nextStagePub.Send(msgBytes)
	return nil
}

func (f *FilterGenericWorker) getClientStats(clientId ClientId) *middleware.ClientStats {
	if _, exists := f.clientsStats[clientId]; !exists {
		f.clientsStats[clientId] = middleware.NewClientStats()
	}
	return f.clientsStats[clientId]
}

func (f *FilterGenericWorker) handleEofMessage(eofMessage amqp.Delivery, eofMsg middleware.Message) {
	f.clientsStatsMutex.Lock()
	clientStats := f.getClientStats(eofMsg.ClientId)
	processed := clientStats.GetProcessed(eofMsg.DataType)
	f.clientsStatsMutex.Unlock()

	f.log.Infof("Received EOF message for client %s and dataType %s. Expecting %d processed messages", eofMsg.ClientId, eofMsg.DataType, eofMsg.TotalEmitted)

	if processed < eofMsg.TotalEmitted {
		f.log.Infof("Not all messages processed yet for client %s and dataType %s", eofMsg.ClientId, eofMsg.DataType)
		f.log.Infof("Waiting for all messages to be processed for client %s and dataType %s", eofMsg.ClientId, eofMsg.DataType)
		clientStats.WaitForEofChan(eofMsg.DataType)
	}

	f.log.Infof("All messages processed for client %s and dataType %s", eofMsg.ClientId, eofMsg.DataType)

	// update emitted count
	f.clientsStatsMutex.Lock()
	eofMsg.TotalEmitted = clientStats.GetEmitted(eofMsg.DataType)
	f.clientsStatsMutex.Unlock()

	err := f.sendNextStage(eofMsg)
	if err != nil {
		f.log.Errorf("Failed to send EOF message to next stage: %v", err)
		answerMessage(NACK_DISCARD, eofMessage)
		return
	}
	answerMessage(ACK, eofMessage)
	f.log.Infof("Sent EOF message to next stage for client %s and dataType %s. Emitted count: %d", eofMsg.ClientId, eofMsg.DataType, eofMsg.TotalEmitted)
}

func (f *FilterGenericWorker) processedCountMessage(message amqp.Delivery) error {
	msg, err := middleware.NewMessageProcessedFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	f.clientsStatsMutex.Lock()
	defer f.clientsStatsMutex.Unlock()
	clientStats := f.getClientStats(msg.ClientId)

	clientStats.AddProcessed(msg.DataType)
	if msg.Emitted {
		clientStats.AddEmitted(msg.DataType)
	}

	if prevEofEmittedCount, exists := clientStats.GetEof(msg.DataType); exists {
		// EOF ARRIVED!
		if clientStats.GetProcessed(msg.DataType) == prevEofEmittedCount {
			f.log.Infof("All messages processed for client %s and dataType %s", msg.ClientId, msg.DataType)
			clientStats.SendEofChan(msg.DataType)
		}
	}

	answerMessage(ACK, message)
	return nil
}

func (f *FilterGenericWorker) Run() error {
	defer f.Shutdown()
	go f.handleSignal()

	if f.conf.ofType == FILTER_TYPE_AMOUNT {
		err := f.createExchangeHandlersForFinalStage()
		if err != nil {
			return fmt.Errorf("failed to create exchange handlers: %v", err)
		}
	} else {
		err := f.createExchangeHandlers()
		if err != nil {
			return fmt.Errorf("failed to create exchange handlers: %v", err)
		}
	}

	f.middlewareHandlers.prevStageSub.StartConsuming(f.filterMessage, f.errChan)
	f.middlewareHandlers.broadcastCountSub.StartConsuming(f.processedCountMessage, f.errChan)

	for err := range f.errChan {
		if err != middleware.MessageMiddlewareSuccess {
			f.log.Errorf("Error found while filtering message of type: %v", err)
		}

		if !f.isRunning {
			f.log.Info("Inside error loop: breaking")
			break
		}
	}

	f.log.Info("Finished filtering")
	return nil
}

// Shutdown gracefully stops the acceptor, closing the listener and current client.
func (f *FilterGenericWorker) Shutdown() {
	f.isRunning = false
	f.errChan <- middleware.MessageMiddlewareSuccess
	f.middlewareHandler.Close()

	f.middlewareHandlers.prevStageSub.Close()
	f.middlewareHandlers.broadcastCountPub.Close()
	f.middlewareHandlers.broadcastCountSub.Close()
	for _, nextStagePub := range f.middlewareHandlers.nextStagePubs {
		nextStagePub.Close()
	}

	f.log.Info("Shutdown complete")
}
