package group

import (
	"common/logger"
	"common/middleware"
	"fmt"
	"group/structures"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type TopkExchangeHandlers struct {
	transactionsGroupedByStoreSubscription middleware.MessageMiddlewareExchange
	transactionsTopKPublishing             middleware.MessageMiddlewareExchange
	eofPublishing                          middleware.MessageMiddlewareExchange
	eofSubscription                        middleware.MessageMiddlewareQueue
}

type GroupByTopKBestClients struct {
	log                       *logging.Logger
	rabbitConn                *middleware.RabbitConnection
	sigChan                   chan os.Signal
	isRunning                 bool
	exchangeHandlers          TopkExchangeHandlers
	errChan                   chan middleware.MessageMiddlewareError
	id                        string
	topKCount                 int
	currentMessageProcessing  middleware.MessageGrouped
	mutex                     sync.Mutex
	eofChan                   chan int
	eofIntercommunicationChan chan int
	topKMap                   map[string]map[string]*structures.Toper[structures.TopKRegister]
	k                         int
}

const (
	ACTIVITY = 0
)

func NewGroupByTopKBestClients(rabbitConf middleware.RabbitConfig, groupById string, groupByCount int, Kconfig int) (*GroupByTopKBestClients, error) {
	log := logger.GetLoggerWithPrefix("[GROUP-TOPK]")

	log.Infof("Establishing connection with RabbitMQ on address %s:%d", rabbitConf.Host, rabbitConf.Port)

	rabbitConn, err := middleware.NewRabbitConnection(&rabbitConf)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	log.Info("Connection with RabbitMQ successfully established")

	sigChan := make(chan os.Signal, SINGLE_ITEM_BUFFER_LEN)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	return &GroupByTopKBestClients{
		log:                       log,
		rabbitConn:                rabbitConn,
		sigChan:                   sigChan,
		isRunning:                 true,
		errChan:                   make(chan middleware.MessageMiddlewareError, ERROR_CHANNEL_BUFFER_SIZE),
		id:                        groupById,
		topKCount:                 groupByCount,
		mutex:                     sync.Mutex{},
		eofChan:                   make(chan int, SINGLE_ITEM_BUFFER_LEN),
		eofIntercommunicationChan: make(chan int, SINGLE_ITEM_BUFFER_LEN),
		topKMap:                   make(map[string]map[string]*structures.Toper[structures.TopKRegister]),
		k:                         Kconfig,
	}, nil
}

func (t *GroupByTopKBestClients) createTopKExchangeHandler() error {
	transactionsGroupedByStoreSubscriptionRouteKey := "transactions.transactions.group.store"
	transactionsGroupedByStoreSubscriptionHandler, err := createExchangeHandler(t.rabbitConn, transactionsGroupedByStoreSubscriptionRouteKey, middleware.EXCHANGE_TYPE_DIRECT)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for transactions.transactions.group.store: %v", err)
	}

	transactionsTopKPublishingRouteKey := "transactions.transactions.topk"
	transactionsTopKPublishingHandler, err := createExchangeHandler(t.rabbitConn, transactionsTopKPublishingRouteKey, middleware.EXCHANGE_TYPE_DIRECT)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for transactions.transactions.topk: %v", err)
	}

	eofPublishingRouteKey := fmt.Sprintf("eof.topk.%s", t.id)
	eofPublishingHandler, err := createExchangeHandler(t.rabbitConn, eofPublishingRouteKey, middleware.EXCHANGE_TYPE_TOPIC)

	if err != nil {
		return fmt.Errorf("failed to create next stage publishing exchange: %w", err)
	}

	eofSubscriptionHandler, err := prepareEofQueue(t.rabbitConn, "topk", t.id)
	if err != nil {
		return fmt.Errorf("error preparing EOF queue for eof.topk: %v", err)
	}

	t.exchangeHandlers = TopkExchangeHandlers{
		transactionsGroupedByStoreSubscription: *transactionsGroupedByStoreSubscriptionHandler,
		transactionsTopKPublishing:             *transactionsTopKPublishingHandler,
		eofPublishing:                          *eofPublishingHandler,
		eofSubscription:                        *eofSubscriptionHandler,
	}

	return nil
}

func (t *GroupByTopKBestClients) Run() error {
	defer t.Shutdown()
	go t.handleSignal()

	err := t.createTopKExchangeHandler()
	if err != nil {
		return fmt.Errorf("failed to create exchange handler: %w", err)
	}

	t.exchangeHandlers.transactionsGroupedByStoreSubscription.StartConsuming(t.processDataMessage, t.errChan)
	t.exchangeHandlers.eofSubscription.StartConsuming(t.processInboundEof, t.errChan)

	for err := range t.errChan {
		if err != middleware.MessageMiddlewareSuccess {
			t.log.Errorf("Error found while executing TopK message of type: %v", err)
		}

		if !t.isRunning {
			t.log.Info("Inside error loop: breaking")
			break
		}
	}

	t.log.Info("Finished executing TopK")
	return nil
}

func (t *GroupByTopKBestClients) processDataMessage(message amqp.Delivery) error {
	defer answerMessage(NACK_DISCARD, message)

	msg, err := middleware.NewMessageGroupedFromBytes(message.Body)
	if err != nil {
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	if msg.IsEof {
		go t.initiateEofCoordination(*msg, message.Body)
		answerMessage(ACK, message)
		return nil
	}

	if len(t.eofChan) > 0 {
		<-t.eofChan
	}

	t.mutex.Lock()
	t.currentMessageProcessing = *msg
	t.currentMessageProcessing.Payload = make(map[string][]string)
	t.mutex.Unlock()

	msgParsed := structures.NewStoreGroupFromMapString(msg.Payload)
	allResultsForClient, storeId := t.getTopK(msgParsed)

	t.mutex.Lock()
	t.topKMap[msg.ClientId] = nil
	t.mutex.Unlock()

	msgToSend := middleware.NewMessageGrouped(msg.DataType, msg.ClientId, allResultsForClient, false)
	msgToSendBytes, err := msgToSend.ToBytes()
	if err != nil {
		return err
	}

	t.exchangeHandlers.transactionsTopKPublishing.Send(msgToSendBytes)
	answerMessage(ACK, message)

	t.log.Infof("Final TopK results sent for store %s", storeId)
	t.eofChan <- THERE_IS_PREVIOUS_MESSAGE
	return nil
}

func (t *GroupByTopKBestClients) getTopK(msg structures.StoreGroup) (map[string][]string, string) {
	result := make(map[string][]string)
	var returnStoreId string = ""

	for storeId, users := range msg {
		if len(users) == 0 {
			continue
		}
		toper := structures.NewToper(t.k, structures.CmpTransactions)
		for userID, value := range users {
			userId := string(userID)
			if userId == "" {
				continue
			}
			count := int(value)
			if count <= 0 {
				continue
			}
			registry := structures.NewTopKRegister(string(storeId), userId, count)
			toper.Add(registry)
		}
		topKUsers := toper.GetTopK()
		result[string(storeId)] = make([]string, 0, len(topKUsers))
		for _, user := range topKUsers {
			result[string(storeId)] = append(result[string(storeId)], user.String())
		}
		returnStoreId = string(storeId)
	}
	return result, returnStoreId
}

func (t *GroupByTopKBestClients) initiateEofCoordination(originalMsg middleware.MessageGrouped, originalMsgBytes []byte) {
	clientId := originalMsg.ClientId
	t.log.Infof("Received EOF message for client %s", clientId)

	totalOfEofs := t.topKCount - 1

	if totalOfEofs == 0 {
		t.log.Infof("No EOF coordination needed for %s", originalMsg.DataType)
	} else {
		t.log.Infof("Coordinating EOF for %s", originalMsg.DataType)
	}

	t.exchangeHandlers.eofPublishing.Send(originalMsgBytes)

	for i := 0; i < totalOfEofs; i++ {
		t.log.Warningf("BEFORE %d %s", i, originalMsg.DataType)
		<-t.eofIntercommunicationChan
		t.log.Warningf("AFTER %d %s", i, originalMsg.DataType)
	}

	t.exchangeHandlers.transactionsTopKPublishing.Send(originalMsgBytes)
	t.log.Warningf("Propagated EOF for %s to next pipeline stage", originalMsg.DataType)
}

func (t *GroupByTopKBestClients) processInboundEof(message amqp.Delivery) error {
	defer answerMessage(NACK_DISCARD, message)

	msg, err := middleware.NewEofMessageGroupedFromBytes(message.Body)
	if err != nil {
		return err
	}
	t.log.Warningf("processInboundEof %s topK%s", msg.DataType, t.id)

	didSomebodyElseAcked := msg.Origin == t.id && msg.IsAck && msg.ImmediateSource != t.id
	if didSomebodyElseAcked {
		t.eofIntercommunicationChan <- ACTIVITY
		return nil
	}

	isAckMine := msg.ImmediateSource == t.id
	isAckForNotForMe := msg.IsAck && msg.Origin != t.id
	if isAckMine || isAckForNotForMe {
		answerMessage(ACK, message)
		return nil
	}

	t.log.Warning("Lock")
	t.mutex.Lock()
	currentMessageProcessing := t.currentMessageProcessing
	t.mutex.Unlock()
	t.log.Warning("Unlock")

	if currentMessageProcessing.IsFromSameStream(msg.DataType, msg.ClientId) {
		t.log.Warningf("BEFORE INBOUND %s", msg.DataType)
		<-t.eofChan
		t.log.Warningf("AFTER INBOUND %s", msg.DataType)
	}

	msg.ImmediateSource = t.id
	msg.IsAck = true
	msgBytes, err := msg.ToBytes()
	if err != nil {
		return err
	}

	answerMessage(ACK, message)
	t.exchangeHandlers.eofPublishing.Send(msgBytes)
	return nil
}

func (t *GroupByTopKBestClients) Shutdown() {
	t.isRunning = false
	t.errChan <- middleware.MessageMiddlewareSuccess

	t.exchangeHandlers.eofSubscription.StopConsuming()
	t.exchangeHandlers.eofSubscription.Close()
	t.exchangeHandlers.eofPublishing.Close()
	t.rabbitConn.Close()

	t.log.Info("Shutdown complete")
}

func (t *GroupByTopKBestClients) handleSignal() {
	<-t.sigChan
	t.log.Info("Handling signal")
	t.Shutdown()
}
