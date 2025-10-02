package topk

import (
	"common/logger"
	"common/middleware"
	"fmt"
	structures "group/structures"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type TopKWorker struct {
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
	eofIntercommunicationChan chan structures.StoreGroup
	topKMap                   map[string]map[string]*Toper[TopKRegister]
}

type TopkExchangeHandlers struct {
	transactionsGroupedByStoreSubscription middleware.MessageMiddlewareExchange
	transactionsTopKPublishing             middleware.MessageMiddlewareExchange
	eofPublishing                          middleware.MessageMiddlewareExchange
	eofSubscription                        middleware.MessageMiddlewareQueue
}

const (
	ERROR_CHANNEL_BUFFER_SIZE = 20
	SINGLE_ITEM_BUFFER_LEN    = 1

	THERE_IS_PREVIOUS_MESSAGE = 0
)

func NewTopKWorker(Kconfig int, topKCount int, id string, rabbitConf middleware.RabbitConfig) (*TopKWorker, error) {
	log := logger.GetLoggerWithPrefix("[TOPK]")

	log.Infof("Establishing connection with RabbitMQ on address %s:%d", rabbitConf.Host, rabbitConf.Port)

	rabbitConn, err := middleware.NewRabbitConnection(&rabbitConf)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	log.Info("Connection with RabbitMQ successfully established")

	sigChan := make(chan os.Signal, SINGLE_ITEM_BUFFER_LEN)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	return &TopKWorker{
		log:                       log,
		rabbitConn:                rabbitConn,
		sigChan:                   sigChan,
		isRunning:                 true,
		errChan:                   make(chan middleware.MessageMiddlewareError, ERROR_CHANNEL_BUFFER_SIZE),
		id:                        id,
		topKCount:                 topKCount,
		mutex:                     sync.Mutex{},
		eofChan:                   make(chan int, SINGLE_ITEM_BUFFER_LEN),
		eofIntercommunicationChan: make(chan structures.StoreGroup, SINGLE_ITEM_BUFFER_LEN),
		topKMap:                   make(map[string]map[string]*Toper[TopKRegister]),
	}, nil
}

func (t *TopKWorker) createTopKExchangeHandler() error {
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

	// dataSub, err := prepareDataQueue(t.rbConn, t.id)
	// if err != nil {
	// 	return fmt.Errorf("error preparing data queue for topk: %v", err)
	// }

	// nextStepPub, err := createExchangeHandler(t.rbConn, "", middleware.EXCHANGE_TYPE_DIRECT)
	// if err != nil {
	// 	return fmt.Errorf("failed to create next stage publishing exchange: %w", err)
	// }

	eofPublishingRouteKey := fmt.Sprintf("eof.topk.%s", t.id)
	eofPublishingHandler, err := createExchangeHandler(t.rabbitConn, eofPublishingRouteKey, middleware.EXCHANGE_TYPE_TOPIC)

	if err != nil {
		return fmt.Errorf("failed to create next stage publishing exchange: %w", err)
	}

	eofSubscriptionHandler, err := prepareEofQueue(t.rabbitConn, t.id)
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

func (t *TopKWorker) Run() error {
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

func (t *TopKWorker) processDataMessage(message amqp.Delivery) error {
	defer answerMessage(ACK, message)

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
	t.mergeMessages(*msg)
	t.mutex.Unlock()
	answerMessage(ACK, message)

	t.eofChan <- THERE_IS_PREVIOUS_MESSAGE

	t.log.Info("TopK'd message")
	return nil
}

func (t *TopKWorker) resultForClient(clientId string) map[string][]string {
	topK := t.topKMap[clientId]
	mapResult := make(map[string][]string)
	for StoreId, toper := range topK {
		mapResult[StoreId] = make([]string, 0)
		topKUsers := toper.GetTopK()
		for _, user := range topKUsers {
			mapResult[StoreId] = append(mapResult[StoreId], user.String())
		}

	}

	// delete(t.topKMap, clientId)

	return mapResult
}

func (t *TopKWorker) initiateEofCoordination(originalMsg middleware.MessageGrouped, originalMsgBytes []byte) {
	clientId := originalMsg.ClientId
	t.log.Infof("Received EOF message for client %s", clientId)

	totalOfEofs := t.topKCount - 1

	if totalOfEofs == 0 {
		t.log.Infof("No EOF coordination needed for %s", originalMsg.DataType)
	} else {
		t.log.Infof("Coordinating EOF for %s", originalMsg.DataType)
	}

	for i := 0; i < totalOfEofs; i++ {
		t.log.Warningf("BEFORE %d %s", i, originalMsg.DataType)
		<-t.eofChan
		t.log.Warningf("AFTER %d %s", i, originalMsg.DataType)
	}

	allResultsForClient := t.resultForClient(clientId)
	t.log.Infof("Final results for client %s: %v", clientId, allResultsForClient)
	// t.exchHandler.eofPub.Send([]byte(allResultsForClient))

}

func (t *TopKWorker) processInboundEof(message amqp.Delivery) error {
	defer answerMessage(ACK, message)

	t.log.Infof("Received inbound EOF message")

	msg, err := middleware.NewEofMessageGroupedFromBytes(message.Body)
	if err != nil {
		return err
	}

	t.log.Warningf("processInboundEof %s groupBy%s", msg.DataType, t.id)

	didSomebodyElseAcked := msg.Origin == t.id && msg.IsAck && msg.ImmediateSource != t.id
	if didSomebodyElseAcked {
		partialTopK := t.resultForClient(msg.ClientId)
		t.log.Infof("Merging partial TopK from %s for client %s: %v", msg.ImmediateSource, msg.ClientId, partialTopK)
		// t.exchHandler.eofPub <- partialTopK
		return nil
	}

	isAckMine := msg.ImmediateSource == t.id
	isAckForNotForMe := msg.IsAck && msg.Origin != t.id
	if isAckMine || isAckForNotForMe {
		answerMessage(ACK, message)
		return nil
	}

	return nil
}

func (t *TopKWorker) mergeMessages(msg middleware.MessageGrouped) {

	cliendId := msg.ClientId
	topKClienteMap := t.topKMap[cliendId]

	data := structures.NewStoreGroupFromMapString(msg.Payload)
	for storeId, users := range data {

		topKClienteStore, exists := topKClienteMap[string(storeId)]
		if !exists {
			topKClienteStore = NewToper(t.topK, CmpTransactions)
			topKClienteMap[string(storeId)] = topKClienteStore
		}

		for userID, value := range users {
			userId := string(userID)
			count := int(value)
			registry := NewTopKRegister(string(storeId), userId, count)
			topKClienteStore.Add(registry)
		}
	}
}

func (t *TopKWorker) Shutdown() {
	t.isRunning = false
	t.errChan <- middleware.MessageMiddlewareSuccess

	t.exchangeHandlers.eofSubscription.StopConsuming()
	t.exchangeHandlers.eofSubscription.Close()
	t.exchangeHandlers.eofPublishing.Close()
	t.rabbitConn.Close()

	t.log.Info("Shutdown complete")
}

func (t *TopKWorker) handleSignal() {
	<-t.sigChan
	t.log.Info("Handling signal")
	t.Shutdown()
}
