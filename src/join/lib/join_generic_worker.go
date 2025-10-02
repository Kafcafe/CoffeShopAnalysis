package join

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

type JoinGenericWorker struct {
	log        *logging.Logger
	rabbitConn *middleware.RabbitConnection
	sigChan    chan os.Signal
	isRunning  bool

	conf                     JoinWorkerConfig
	middlewareHandlers       JoinMiddlewareHandlers
	errChan                  chan middleware.MessageMiddlewareError
	currentMessageProcessing middleware.Message
	mutex                    sync.Mutex

	eofChan                   chan int
	eofIntercommunicationChan chan int

	sideTable         []string
	sideTableReceived chan int
}

type JoinMiddlewareHandlers struct {
	prevStageSub middleware.MessageMiddlewareExchange
	sideTableSub middleware.MessageMiddlewareQueue
	nextStagePub middleware.MessageMiddlewareExchange
	eofPub       middleware.MessageMiddlewareExchange
	eofSub       middleware.MessageMiddlewareQueue
}

func (mh *JoinMiddlewareHandlers) Shutdown() {
	mh.prevStageSub.Close()
	mh.sideTableSub.Close()
	mh.nextStagePub.Close()
	mh.eofPub.Close()
	mh.eofSub.Close()
}

// handleSignal listens for SIGTERM signal and triggers shutdown.
func (j *JoinGenericWorker) handleSignal() {
	<-j.sigChan
	j.log.Info("Handling signal")
	j.Shutdown()
}

func NewJoinWorker(rabbitConf middleware.RabbitConfig, config JoinWorkerConfig) (*JoinGenericWorker, error) {
	log := logger.GetLoggerWithPrefix("[JOINER-ITEMS-" + config.id + "] ")

	log.Infof("Establishing connection with RabbitMQ on address %s:%d", rabbitConf.Host, rabbitConf.Port)

	rabbitConn, err := middleware.NewRabbitConnection(&rabbitConf)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	log.Info("Connection with RabbitMQ successfully established")

	sigChan := make(chan os.Signal, SINGLE_ITEM_BUFFER_LEN)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	return &JoinGenericWorker{
		log:        log,
		rabbitConn: rabbitConn,
		sigChan:    sigChan,
		isRunning:  true,

		conf:                      config,
		errChan:                   make(chan middleware.MessageMiddlewareError, ERROR_CHANNEL_BUFFER_SIZE),
		mutex:                     sync.Mutex{},
		eofChan:                   make(chan int, SINGLE_ITEM_BUFFER_LEN),
		eofIntercommunicationChan: make(chan int, SINGLE_ITEM_BUFFER_LEN),
		sideTable:                 make([]string, 0),
		sideTableReceived:         make(chan int, SINGLE_ITEM_BUFFER_LEN),
	}, nil
}

func (j *JoinGenericWorker) createExchangeHandlers() error {
	prevStageSub, err := createExchangeHandler(j.rabbitConn, j.conf.prevStageSub, middleware.EXCHANGE_TYPE_DIRECT)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for transactions: %v", err)
	}

	queueName := fmt.Sprintf("%s.%s.%s", j.conf.sideTableSub, j.conf.ofType, j.conf.id)
	sideTableSub, err := prepareSideTableQueue(j.rabbitConn, queueName, j.conf.sideTableSub)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for transactions: %v", err)
	}

	nextStagePub, err := createExchangeHandler(j.rabbitConn, j.conf.nextStagePub, middleware.EXCHANGE_TYPE_TOPIC)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for results.q2: %v", err)
	}

	eofPubRouteKey := fmt.Sprintf("eof.join.items.%s", j.conf.id)
	eofPub, err := createExchangeHandler(j.rabbitConn, eofPubRouteKey, middleware.EXCHANGE_TYPE_TOPIC)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for eof.join.items: %v", err)
	}

	eofSub, err := prepareEofQueue(j.rabbitConn, "items", j.conf.id)
	if err != nil {
		return fmt.Errorf("Error preparing EOF queue for eof.join.items: %v", err)
	}

	j.middlewareHandlers = JoinMiddlewareHandlers{
		prevStageSub: *prevStageSub,
		sideTableSub: *sideTableSub,
		nextStagePub: *nextStagePub,
		eofPub:       *eofPub,
		eofSub:       *eofSub,
	}
	return nil
}

func (j *JoinGenericWorker) initiateEofCoordination(originalMsg middleware.Message, originalMsgBytes []byte) {
	eofMsg := middleware.NewEofMessage(originalMsg.DataType, originalMsg.ClientId, j.conf.id, j.conf.id, false)
	msgBytes, err := eofMsg.ToBytes()
	if err != nil {
		j.log.Errorf("Failed to serialize message: %v", err)
	}

	j.middlewareHandlers.eofPub.Send(msgBytes)

	totalEofs := j.conf.count - 1

	if totalEofs == 0 {
		j.log.Infof("No EOF coordination needed for %s", originalMsg.DataType)
	} else {
		j.log.Infof("Coordinating EOF for %s", originalMsg.DataType)
	}

	for i := 0; i < totalEofs; i++ {
		j.log.Warningf("BEFORE %d %s", i, originalMsg.DataType)
		<-j.eofIntercommunicationChan
		j.log.Warningf("AFTER %d %s", i, originalMsg.DataType)
	}

	middleError := j.middlewareHandlers.nextStagePub.Send(originalMsgBytes)
	if middleError != middleware.MessageMiddlewareSuccess {
		j.log.Error("problem while sending message to resultsQ2Publishing")
	}

	j.log.Warningf("Propagated EOF for %s to next pipeline stage: %s", originalMsg.DataType, j.conf.nextStagePub)
}

func (j *JoinGenericWorker) joinWithSideTable(message amqp.Delivery) error {
	defer answerMessage(NACK_DISCARD, message)

	msg, err := middleware.NewMessageGroupedFromBytes(message.Body)
	if err != nil {
		return err
	}

	if msg.IsEof {
		go j.initiateEofCoordination(*msg.ToEmptyMessage(), message.Body)
		answerMessage(ACK, message)
		return nil
	}

	if len(j.eofChan) > 0 {
		<-j.eofChan
	}

	j.mutex.Lock()
	j.currentMessageProcessing = *msg.ToEmptyMessage()
	j.mutex.Unlock()

	j.log.Debugf("Received payload: %v", msg.Payload)

	joined := j.conf.messageCallback(NewJoiner(), j.sideTable, msg.Payload)

	j.log.Infof("Joined %v", joined)

	isEof := false
	response := middleware.NewMessage(msg.DataType, msg.ClientId, joined, isEof)
	responseBytes, err := response.ToBytes()
	if err != nil {
		return err
	}

	j.middlewareHandlers.nextStagePub.Send(responseBytes)
	answerMessage(ACK, message)
	j.eofChan <- THERE_IS_PREVIOUS_MESSAGE

	j.log.Infof("Joined message and sent to next stage: %s", j.conf.nextStagePub)
	return nil
}

func (j *JoinGenericWorker) processInboundEof(message amqp.Delivery) error {
	defer answerMessage(NACK_DISCARD, message)

	msg, err := middleware.NewEofMessageFromBytes(message.Body)
	if err != nil {
		return err
	}
	j.log.Warningf("processInboundEof %s join%s", msg.DataType, j.conf.id)

	didSomebodyElseAcked := msg.Origin == j.conf.id && msg.IsAck && msg.ImmediateSource != j.conf.id
	if didSomebodyElseAcked {
		j.eofIntercommunicationChan <- ACTIVITY
		return nil
	}

	isAckMine := msg.ImmediateSource == j.conf.id
	isAckForNotForMe := msg.IsAck && msg.Origin != j.conf.id
	if isAckMine || isAckForNotForMe {
		answerMessage(ACK, message)
		return nil
	}

	j.log.Warning("Lock")
	j.mutex.Lock()
	currentMessageProcessing := j.currentMessageProcessing
	j.mutex.Unlock()
	j.log.Warning("Unlock")

	if currentMessageProcessing.IsFromSameStream(msg.DataType, msg.ClientId) {
		j.log.Warningf("BEFORE INBOUND %s", msg.DataType)
		<-j.eofChan
		j.log.Warningf("AFTER INBOUND %s", msg.DataType)
	}

	msg.ImmediateSource = j.conf.id
	msg.IsAck = true
	msgBytes, err := msg.ToBytes()
	if err != nil {
		return err
	}

	answerMessage(ACK, message)
	j.middlewareHandlers.eofPub.Send(msgBytes)
	return nil
}

func (j *JoinGenericWorker) storeSideTable(message amqp.Delivery) error {
	defer answerMessage(NACK_DISCARD, message)

	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		return err
	}

	if msg.IsEof {
		j.log.Info("Received EOF for menu items. Ready to Join.")
		answerMessage(ACK, message)
		j.sideTableReceived <- ACTIVITY
		// j.middlewareHandlers.sideTableSub.StopConsuming()
		return nil
	}

	j.sideTable = msg.Payload

	j.log.Infof("Stored %d side table: %v", len(msg.Payload), msg.Payload)
	answerMessage(ACK, message)
	return nil
}

func (j *JoinGenericWorker) Run() error {
	defer j.Shutdown()
	go j.handleSignal()

	err := j.createExchangeHandlers()
	if err != nil {
		return fmt.Errorf("failed to create exchange handlers: %v", err)
	}

	j.log.Info("Waiting to receive side table...")
	j.middlewareHandlers.sideTableSub.StartConsuming(j.storeSideTable, j.errChan)
	<-j.sideTableReceived

	if !j.isRunning {
		return nil
	}

	j.middlewareHandlers.prevStageSub.StartConsuming(j.joinWithSideTable, j.errChan)
	j.middlewareHandlers.eofSub.StartConsuming(j.processInboundEof, j.errChan)

	j.log.Info("Started consuming messages. Ready to join!")
	for err := range j.errChan {
		if err != middleware.MessageMiddlewareSuccess {
			j.log.Errorf("Error found while joining message of type: %v", err)
		}

		if !j.isRunning {
			j.log.Info("Inside error loop: breaking")
			break
		}
	}

	j.log.Info("Finished joining!")
	return nil
}

// Shutdown gracefully stops the acceptor, closing the listener and current client.
func (j *JoinGenericWorker) Shutdown() {
	j.isRunning = false
	j.sideTableReceived <- ACTIVITY
	j.errChan <- middleware.MessageMiddlewareSuccess

	j.middlewareHandlers.Shutdown()
	j.rabbitConn.Close()

	j.log.Info("Shutdown complete")
}
