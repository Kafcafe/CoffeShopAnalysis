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
	prevStageSubscription      middleware.MessageMiddlewareQueue
	transactionsTopKPublishing middleware.MessageMiddlewareExchange
	eofPublishing              middleware.MessageMiddlewareExchange
	eofSubscription            middleware.MessageMiddlewareQueue
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
	currentMessageProcessing  middleware.Message
	mutex                     sync.Mutex
	eofChan                   chan int
	eofIntercommunicationChan chan structures.StoreGroup
	topKMap                   map[string]map[string]*structures.Toper[structures.TopKRegister]
	groupedPerClient          structures.StoreGroupPerClient
	k                         int
	middlewareHandler         *middleware.MiddlewareHandler
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

	middlewareHandler, err := middleware.NewMiddlewareHandler(rabbitConn)
	if err != nil {
		return nil, fmt.Errorf("failed to create middleware handler: %v", err)
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
		eofIntercommunicationChan: make(chan structures.StoreGroup, SINGLE_ITEM_BUFFER_LEN),
		topKMap:                   make(map[string]map[string]*structures.Toper[structures.TopKRegister]),
		k:                         Kconfig,
		middlewareHandler:         middlewareHandler,
		groupedPerClient:          structures.NewStoreGroupPerClient(),
	}, nil
}

func (t *GroupByTopKBestClients) createTopKExchangeHandler() error {
	prevStageSub := "transactions.transactions.all"
	_, err := t.middlewareHandler.CreateDirectExchangeStandalone(prevStageSub)
	if err != nil {
		return fmt.Errorf("Error creating exchange handler for transactions.year-hour-filtered.all: %v", err)
	}

	prevStageSubQueueName := prevStageSub + ".topk"
	prevStageSubscription, err := t.middlewareHandler.CreateQueue(prevStageSubQueueName)
	if err != nil {
		return fmt.Errorf("Error creating queue handler for %s: %v", prevStageSubQueueName, err)
	}

	err = t.middlewareHandler.BindQueue(prevStageSubQueueName, middleware.EXCHANGE_NAME_DIRECT_TYPE, prevStageSub)
	if err != nil {
		return fmt.Errorf("Error preparing queue for %s: %v", prevStageSubQueueName, err)
	}

	prepareInputQueues(t.rabbitConn, "store")

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
		prevStageSubscription:      *prevStageSubscription,
		transactionsTopKPublishing: *transactionsTopKPublishingHandler,
		eofPublishing:              *eofPublishingHandler,
		eofSubscription:            *eofSubscriptionHandler,
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

	t.exchangeHandlers.prevStageSubscription.StartConsuming(t.groupByStore, t.errChan)
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

func (t *GroupByTopKBestClients) groupByStore(message amqp.Delivery) error {
	defer answerMessage(NACK_DISCARD, message)

	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		return err
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
	t.currentMessageProcessing.Payload = []string{}

	t.groupedPerClient.Add(msg.ClientId, msg.Payload)
	t.mutex.Unlock()
	answerMessage(ACK, message)

	t.eofChan <- THERE_IS_PREVIOUS_MESSAGE

	t.log.Info("Grouped message")
	return nil
}

func (t *GroupByTopKBestClients) getTopK(msg *structures.StoreGroup) (map[string][]string, string) {
	result := make(map[string][]string)
	var returnStoreId string = ""

	for storeId, users := range *msg {
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
			result[string(storeId)] = append(result[string(storeId)], fmt.Sprintf("%s", user.String()))
		}
		returnStoreId = string(storeId)
	}
	return result, returnStoreId
}

func bytesToMB(bytes int) float64 {
	const bytesInMB = 1024 * 1024 // 1 MB = 1024 * 1024 bytes
	return float64(int64(bytes)) / float64(bytesInMB)
}

func (t *GroupByTopKBestClients) initiateEofCoordination(originalMsg middleware.Message, originalMsgBytes []byte) {
	eofMsg := middleware.NewEofMessageGrouped(originalMsg.DataType, originalMsg.ClientId, t.id, t.id, false, nil)
	msgBytes, err := eofMsg.ToBytes()
	if err != nil {
		t.log.Errorf("Failed to serialize message: %v", err)
	}

	t.exchangeHandlers.eofPublishing.Send(msgBytes)

	totalEofs := t.topKCount - 1

	if totalEofs == 0 {
		t.log.Infof("No EOF coordination needed for %s", originalMsg.DataType)
	} else {
		t.log.Infof("Coordinating EOF for %s", originalMsg.DataType)
	}

	t.log.Infof("Consolidating partial results for %s", originalMsg.DataType)

	t.mutex.Lock()
	clientStoreGroup := t.groupedPerClient.Get(originalMsg.ClientId)

	t.groupedPerClient.Delete(originalMsg.ClientId)
	t.mutex.Unlock()

	myPayload, _ := middleware.NewMessageGrouped(originalMsg.DataType, originalMsg.ClientId, clientStoreGroup.ToMapString(), false).ToBytes()
	t.log.Infof("Partial grouping from %s has size: %.4fMB", t.id, bytesToMB(len(myPayload)))

	/*
	* Collect partial groupings from other nodes
	 */
	for i := 0; i < totalEofs; i++ {
		t.log.Warningf("BEFORE %d %s", i, originalMsg.DataType)
		partialGrouping := <-t.eofIntercommunicationChan
		clientStoreGroup.Merge(partialGrouping)
		t.log.Warningf("AFTER %d %s", i, originalMsg.DataType)
	}

	/*
	* Calculate TopK and send results
	 */

	allResultsForClient, _ := t.getTopK(&clientStoreGroup)

	t.log.Infof("VALUES %v", allResultsForClient)

	msgToSend := middleware.NewMessageGrouped(originalMsg.DataType, originalMsg.ClientId, allResultsForClient, false)
	msgToSendBytes, err := msgToSend.ToBytes()
	if err != nil {
		t.log.Errorf("Failed to serialize message: %v", err)
		return
	}

	size := len(msgToSendBytes)
	t.exchangeHandlers.transactionsTopKPublishing.Send(msgToSendBytes)
	t.log.Infof("Final TopK results size: %.4fMB", bytesToMB(size))
	t.log.Infof("Final TopK results sent")

	/*
	* Propagate EOF
	 */

	middleError := t.exchangeHandlers.transactionsTopKPublishing.Send(originalMsgBytes)
	if middleError != middleware.MessageMiddlewareSuccess {
		t.log.Errorf("problem while propagating EOF")
	}

	t.log.Warningf("Propagated EOF for %s to next pipeline stage", originalMsg.DataType)
}

func (g *GroupByTopKBestClients) processInboundEof(message amqp.Delivery) error {
	defer answerMessage(NACK_DISCARD, message)

	msg, err := middleware.NewEofMessageGroupedFromBytes(message.Body)
	if err != nil {
		return err
	}
	g.log.Warningf("processInboundEof %s groupBy%s", msg.DataType, g.id)

	didSomebodyElseAcked := msg.Origin == g.id && msg.IsAck && msg.ImmediateSource != g.id
	if didSomebodyElseAcked {
		size := len(msg.Payload)
		g.log.Infof("Partial grouping from %s has size: %.4fMB", msg.ImmediateSource, bytesToMB(size))
		partialGrouping := structures.NewStoreGroupFromMapString(msg.Payload)
		g.eofIntercommunicationChan <- partialGrouping
		return nil
	}

	isAckMine := msg.ImmediateSource == g.id
	isAckForNotForMe := msg.IsAck && msg.Origin != g.id
	if isAckMine || isAckForNotForMe {
		answerMessage(ACK, message)
		return nil
	}

	g.log.Warning("Lock")
	g.mutex.Lock()
	currentMessageProcessing := g.currentMessageProcessing
	g.mutex.Unlock()
	g.log.Warning("Unlock")

	if currentMessageProcessing.IsFromSameStream(msg.DataType, msg.ClientId) {
		g.log.Warningf("BEFORE INBOUND %s", msg.DataType)
		<-g.eofChan
		g.log.Warningf("AFTER INBOUND %s", msg.DataType)
	}

	msg.ImmediateSource = g.id
	msg.IsAck = true

	g.mutex.Lock()
	msg.Payload = g.groupedPerClient.Get(msg.ClientId).ToMapString()
	g.mutex.Unlock()

	msgBytes, err := msg.ToBytes()
	if err != nil {
		return err
	}

	answerMessage(ACK, message)
	g.exchangeHandlers.eofPublishing.Send(msgBytes)
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
