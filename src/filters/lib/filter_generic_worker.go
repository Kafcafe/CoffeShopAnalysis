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

type FilterGenericWorker struct {
	log               *logging.Logger
	middlewareHandler *middleware.MiddlewareHandler
	sigChan           chan os.Signal
	isRunning         bool

	filter                   Filter
	conf                     FilterConfig
	middlewareHandlers       MiddlewareHandlers
	errChan                  chan middleware.MessageMiddlewareError
	currentMessageProcessing middleware.Message
	mutex                    sync.Mutex

	eofChan                   chan int
	eofIntercommunicationChan chan int
}

type MiddlewareHandlers struct {
	prevStageSub  middleware.MessageMiddlewareQueue // consider a queue
	nextStagePubs map[string]middleware.MessageMiddlewareQueue
	eofPub        middleware.MessageMiddlewareExchange
	eofSub        middleware.MessageMiddlewareQueue
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
		log:                       log,
		middlewareHandler:         middlewareHandler,
		sigChan:                   sigChan,
		isRunning:                 true,
		filter:                    *NewFilter(),
		conf:                      config,
		errChan:                   make(chan middleware.MessageMiddlewareError, ERROR_CHANNEL_BUFFER_SIZE),
		mutex:                     sync.Mutex{},
		eofChan:                   make(chan int, SINGLE_ITEM_BUFFER_LEN),
		eofIntercommunicationChan: make(chan int, SINGLE_ITEM_BUFFER_LEN),
	}, nil
}

func (f *FilterGenericWorker) createExchangeHandlers() error {
	prevStageSub, err := f.middlewareHandler.CreateQueue(f.conf.prevStageSub)
	if err != nil {
		return fmt.Errorf("Error creating queue handler for %s: %v", f.conf.prevStageSub, err)
	}

	nextStagePubs := make(map[string]middleware.MessageMiddlewareQueue)
	for datatype, routeKey := range f.conf.nextStagePubs {
		f.log.Infof("Next stage publishing for datatype %s on routeKey %s", datatype, routeKey)
		queue, err := f.middlewareHandler.CreateQueue(routeKey)
		if err != nil {
			return fmt.Errorf("Error creating exchange handler for %s: %v", routeKey, err)
		}
		nextStagePubs[datatype] = *queue
	}

	f.log.Infof("Setting up EOF coordination PUB for filter %s", f.conf.id)
	eofPublishingRouteKey := fmt.Sprintf("eof.filters.%s", f.conf.ofType)
	eofPub, err := f.middlewareHandler.CreateFanoutExchangeStandalone(eofPublishingRouteKey)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for %s: %v", eofPublishingRouteKey, err)
	}

	f.log.Infof("Setting up EOF coordination SUB for filter %s", f.conf.id)
	eofSubQueueName := fmt.Sprintf("eof.filters.%s.%s", f.conf.ofType, f.conf.id)
	eofSub, err := f.middlewareHandler.CreateQueue(eofSubQueueName)
	if err != nil {
		return fmt.Errorf("Error creating EOF queue for %s: %v", eofSubQueueName, err)
	}
	err = f.middlewareHandler.BindQueue(eofSubQueueName, eofPublishingRouteKey, "")

	if err != nil {
		return fmt.Errorf("Error preparing EOF queue for transactions: %v", err)
	}

	f.middlewareHandlers = MiddlewareHandlers{
		prevStageSub:  *prevStageSub,
		nextStagePubs: nextStagePubs,
		eofPub:        *eofPub,
		eofSub:        *eofSub,
	}
	return nil
}

func (f *FilterGenericWorker) initiateEofCoordination(originalMsg middleware.Message, originalMsgBytes []byte) {
	eofMsg := middleware.NewEofMessage(originalMsg.DataType, originalMsg.ClientId, f.conf.id, f.conf.id, false)
	msgBytes, err := eofMsg.ToBytes()
	if err != nil {
		f.log.Errorf("Failed to serialize message: %v", err)
	}

	f.middlewareHandlers.eofPub.Send(msgBytes)

	totalEofs := f.conf.filtersCount - 1

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

	nextStagePub, exists := f.middlewareHandlers.nextStagePubs[originalMsg.DataType]
	if !exists {
		f.log.Errorf("Unknown dataType '%s' on initiateEofCoordination", originalMsg.DataType)
		return
	}
	nextStagePub.Send(originalMsgBytes)

	f.log.Warningf("Propagated EOF for %s to next pipeline stage", originalMsg.DataType)
}

func (f *FilterGenericWorker) filterMessage(message amqp.Delivery) error {
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

	filteredBatch := f.conf.messageCallback(&f.filter, msg.Payload)

	if len(filteredBatch) == 0 {
		f.log.Info("No transaction passed the filterMessage of type " + f.conf.ofType)
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

	// Send the filtered response to the appropriate exchange
	nextStagePub, exists := f.middlewareHandlers.nextStagePubs[msg.DataType]
	if !exists {
		f.eofChan <- THERE_IS_PREVIOUS_MESSAGE

		return fmt.Errorf("received unprocessabble message in filterMessage of type %s", msg.DataType)
	}
	nextStagePub.Send(responseBytes)
	answerMessage(ACK, message)

	f.eofChan <- THERE_IS_PREVIOUS_MESSAGE

	f.log.Infof("Filtered message and sent filterMessage batch")
	return nil
}

func (f *FilterGenericWorker) processInboundEof(message amqp.Delivery) error {
	defer answerMessage(NACK_DISCARD, message)

	msg, err := middleware.NewEofMessageFromBytes(message.Body)
	if err != nil {
		return err
	}
	f.log.Warningf("processInboundEof %s filter%s", msg.DataType, f.conf.id)

	didSomebodyElseAcked := msg.Origin == f.conf.id && msg.IsAck && msg.ImmediateSource != f.conf.id
	if didSomebodyElseAcked {
		f.eofIntercommunicationChan <- ACTIVITY
		return nil
	}

	isAckMine := msg.ImmediateSource == f.conf.id
	isAckForNotForMe := msg.IsAck && msg.Origin != f.conf.id
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

	msg.ImmediateSource = f.conf.id
	msg.IsAck = true
	msgBytes, err := msg.ToBytes()
	if err != nil {
		return err
	}

	answerMessage(ACK, message)
	f.middlewareHandlers.eofPub.Send(msgBytes)
	return nil
}

func (f *FilterGenericWorker) Run() error {
	defer f.Shutdown()
	go f.handleSignal()

	err := f.createExchangeHandlers()
	if err != nil {
		return fmt.Errorf("failed to create exchange handlers: %v", err)
	}

	f.middlewareHandlers.prevStageSub.StartConsuming(f.filterMessage, f.errChan)
	f.middlewareHandlers.eofSub.StartConsuming(f.processInboundEof, f.errChan)

	for err := range f.errChan {
		if err != middleware.MessageMiddlewareSuccess {
			f.log.Errorf("Error found while filtering message of type: %v", err)
		}

		if !f.isRunning {
			f.log.Info("Inside error loop: breaking")
			break
		}
	}

	f.middlewareHandlers.prevStageSub.Close()
	f.middlewareHandlers.eofSub.StopConsuming()
	f.middlewareHandlers.eofSub.Close()
	f.middlewareHandlers.eofPub.Close()

	f.log.Info("Finished filtering")
	return nil
}

// Shutdown gracefully stops the acceptor, closing the listener and current client.
func (f *FilterGenericWorker) Shutdown() {
	f.isRunning = false
	f.errChan <- middleware.MessageMiddlewareSuccess
	f.middlewareHandler.Close()

	f.middlewareHandlers.prevStageSub.Close()
	f.middlewareHandlers.eofSub.StopConsuming()
	f.middlewareHandlers.eofSub.Close()
	f.middlewareHandlers.eofPub.Close()

	f.log.Info("Shutdown complete")
}
