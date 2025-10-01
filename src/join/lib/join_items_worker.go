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

type JoinerItemsWorker struct {
	log        *logging.Logger
	rabbitConn *middleware.RabbitConnection
	sigChan    chan os.Signal
	isRunning  bool
	// conf                      YearFilterConfig
	exchangeHandlers          JoinExchangeHandlers
	errChan                   chan middleware.MessageMiddlewareError
	id                        string
	joinersCount              int
	currentMessageProcessing  middleware.Message
	mutex                     sync.Mutex
	eofChan                   chan int
	eofIntercommunicationChan chan int
	menuItems                 []string
	hasMenuItems              chan int
}

type JoinExchangeHandlers struct {
	groupYearMonthSubscribing middleware.MessageMiddlewareExchange
	menuItemsSubscribing      middleware.MessageMiddlewareQueue
	resultsQ2Publishing       middleware.MessageMiddlewareExchange
	eofPublishing             middleware.MessageMiddlewareExchange
	eofSubscription           middleware.MessageMiddlewareQueue
}

// handleSignal listens for SIGTERM signal and triggers shutdown.
func (j *JoinerItemsWorker) handleSignal() {
	<-j.sigChan
	j.log.Info("Handling signal")
	j.Shutdown()
}

func NewJoinItemsWorker(rabbitConf middleware.RabbitConfig /* joinerConfig JoinerConfig, */, joinerId string, joinersCount int) (*JoinerItemsWorker, error) {
	log := logger.GetLoggerWithPrefix("[JOINER-ITEMS]")

	log.Infof("Establishing connection with RabbitMQ on address %s:%d", rabbitConf.Host, rabbitConf.Port)

	rabbitConn, err := middleware.NewRabbitConnection(&rabbitConf)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	log.Info("Connection with RabbitMQ successfully established")

	sigChan := make(chan os.Signal, SINGLE_ITEM_BUFFER_LEN)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	return &JoinerItemsWorker{
		log:        log,
		rabbitConn: rabbitConn,
		sigChan:    sigChan,
		isRunning:  true,
		// conf:                      filtersConfig,
		errChan:                   make(chan middleware.MessageMiddlewareError, ERROR_CHANNEL_BUFFER_SIZE),
		id:                        joinerId,
		joinersCount:              joinersCount,
		mutex:                     sync.Mutex{},
		eofChan:                   make(chan int, SINGLE_ITEM_BUFFER_LEN),
		eofIntercommunicationChan: make(chan int, SINGLE_ITEM_BUFFER_LEN),
		hasMenuItems:              make(chan int, SINGLE_ITEM_BUFFER_LEN),
	}, nil
}

func (j *JoinerItemsWorker) createExchangeHandlers() error {
	groupYearMonthRouteKey := "transactions.items.group.yearmonth"
	groupYearMonthSubscribingHandler, err := createExchangeHandler(j.rabbitConn, groupYearMonthRouteKey, middleware.EXCHANGE_TYPE_DIRECT)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for transactions: %v", err)
	}

	menuItemsSubscribingSubscribingHandler, err := prepareMenuItemsQueue(j.rabbitConn, "items", j.id)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for transactions: %v", err)
	}

	resultsQ2PublishingRouteKey := "results.q2"
	resultsQ2PublishingHandler, err := createExchangeHandler(j.rabbitConn, resultsQ2PublishingRouteKey, middleware.EXCHANGE_TYPE_TOPIC)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for results.q2: %v", err)
	}

	eofPublishingRouteKey := fmt.Sprintf("eof.join.items.%s", j.id)
	eofPublishingHandler, err := createExchangeHandler(j.rabbitConn, eofPublishingRouteKey, middleware.EXCHANGE_TYPE_TOPIC)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for eof.join.items: %v", err)
	}

	eofSubscription, err := prepareEofQueue(j.rabbitConn, "items", j.id)
	if err != nil {
		return fmt.Errorf("Error preparing EOF queue for eof.join.items: %v", err)
	}

	j.exchangeHandlers = JoinExchangeHandlers{
		groupYearMonthSubscribing: *groupYearMonthSubscribingHandler,
		menuItemsSubscribing:      *menuItemsSubscribingSubscribingHandler,
		resultsQ2Publishing:       *resultsQ2PublishingHandler,
		eofPublishing:             *eofPublishingHandler,
		eofSubscription:           *eofSubscription,
	}
	return nil
}

func (j *JoinerItemsWorker) initiateEofCoordination(originalMsg middleware.Message, originalMsgBytes []byte) {
	eofMsg := middleware.NewEofMessage(originalMsg.DataType, originalMsg.ClientId, j.id, j.id, false)
	msgBytes, err := eofMsg.ToBytes()
	if err != nil {
		j.log.Errorf("Failed to serialize message: %v", err)
	}

	j.exchangeHandlers.eofPublishing.Send(msgBytes)

	totalEofs := j.joinersCount - 1

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

	middleError := j.exchangeHandlers.resultsQ2Publishing.Send(originalMsgBytes)
	if middleError != middleware.MessageMiddlewareSuccess {
		j.log.Error("problem while sending message to resultsQ2Publishing")
	}

	j.log.Warningf("Propagated EOF for %s to next pipeline stage", originalMsg.DataType)
}

func (j *JoinerItemsWorker) joinWithMenuItems(message amqp.Delivery) error {
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
	flattenedItems := make([]string, 0)
	for yearMonth, items := range msg.Payload {
		for _, item := range items {
			flattenedItems = append(flattenedItems, fmt.Sprintf("%s,%s", yearMonth, item))
		}
	}

	joiner := NewJoiner()
	joinedItems := joiner.JoinByIndex(j.menuItems, flattenedItems, 1, 0, 1)

	j.log.Infof("Joined items %v", joinedItems)

	isEof := false
	response := middleware.NewMessage(msg.DataType, msg.ClientId, joinedItems, isEof)
	responseBytes, err := response.ToBytes()
	if err != nil {
		return err
	}

	j.exchangeHandlers.resultsQ2Publishing.Send(responseBytes)
	answerMessage(ACK, message)
	j.eofChan <- THERE_IS_PREVIOUS_MESSAGE

	j.log.Infof("Joined message and sent to results Q2")
	return nil
}

func (j *JoinerItemsWorker) processInboundEof(message amqp.Delivery) error {
	defer answerMessage(NACK_DISCARD, message)

	msg, err := middleware.NewEofMessageFromBytes(message.Body)
	if err != nil {
		return err
	}
	j.log.Warningf("processInboundEof %s join%s", msg.DataType, j.id)

	didSomebodyElseAcked := msg.Origin == j.id && msg.IsAck && msg.ImmediateSource != j.id
	if didSomebodyElseAcked {
		j.eofIntercommunicationChan <- ACTIVITY
		return nil
	}

	isAckMine := msg.ImmediateSource == j.id
	isAckForNotForMe := msg.IsAck && msg.Origin != j.id
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

	msg.ImmediateSource = j.id
	msg.IsAck = true
	msgBytes, err := msg.ToBytes()
	if err != nil {
		return err
	}

	answerMessage(ACK, message)
	j.exchangeHandlers.eofPublishing.Send(msgBytes)
	return nil
}

func (j *JoinerItemsWorker) storeMenuItems(message amqp.Delivery) error {
	defer answerMessage(NACK_DISCARD, message)

	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		return err
	}

	if msg.IsEof {
		j.log.Info("Received EOF for menu items. Ready to Join.")
		answerMessage(ACK, message)
		j.hasMenuItems <- ACTIVITY
		j.exchangeHandlers.menuItemsSubscribing.StopConsuming()
		return nil
	}

	j.menuItems = msg.Payload

	j.log.Infof("Stored %d menu items: %v", len(msg.Payload), msg.Payload)
	answerMessage(ACK, message)
	return nil
}

func (j *JoinerItemsWorker) Run() error {
	defer j.Shutdown()
	go j.handleSignal()

	err := j.createExchangeHandlers()
	if err != nil {
		return fmt.Errorf("failed to create exchange handlers: %v", err)
	}

	j.log.Info("Waiting to receive menu items...")
	j.exchangeHandlers.menuItemsSubscribing.StartConsuming(j.storeMenuItems, j.errChan)
	<-j.hasMenuItems
	j.exchangeHandlers.groupYearMonthSubscribing.StartConsuming(j.joinWithMenuItems, j.errChan)
	j.exchangeHandlers.eofSubscription.StartConsuming(j.processInboundEof, j.errChan)

	for err := range j.errChan {
		if err != middleware.MessageMiddlewareSuccess {
			j.log.Errorf("Error found while joining message of type: %v", err)
		}

		if !j.isRunning {
			j.log.Info("Inside error loop: breaking")
			break
		}
	}

	j.exchangeHandlers.groupYearMonthSubscribing.Close()
	j.exchangeHandlers.eofSubscription.StopConsuming()
	j.exchangeHandlers.eofSubscription.Close()
	j.exchangeHandlers.eofPublishing.Close()

	j.log.Info("Finished joining!")
	return nil
}

// Shutdown gracefully stops the acceptor, closing the listener and current client.
func (j *JoinerItemsWorker) Shutdown() {
	j.isRunning = false
	j.errChan <- middleware.MessageMiddlewareSuccess
	j.rabbitConn.Close()

	j.exchangeHandlers.groupYearMonthSubscribing.Close()
	j.exchangeHandlers.menuItemsSubscribing.Close()
	j.exchangeHandlers.eofSubscription.StopConsuming()
	j.exchangeHandlers.eofSubscription.Close()
	j.exchangeHandlers.eofPublishing.Close()

	j.log.Info("Shutdown complete")
}
