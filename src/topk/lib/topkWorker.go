package topk

import (
	"common/logger"
	"common/middleware"
	"fmt"
	structures "group/structures"
	"os"
	"os/signal"
	"syscall"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type TopKWorker struct {
	id          string
	sigChan     chan os.Signal
	topK        int
	rbConn      *middleware.RabbitConnection
	exchHandler *TopKMiddlewareHandler
	errChan     chan middleware.MessageMiddlewareError
	topKMap     map[string]map[string]*Toper[TopKRegister]
	log         *logging.Logger
	total       int
	eofChan     chan bool
}

type TopKMiddlewareHandler struct {
	eofPub      middleware.MessageMiddlewareExchange
	eofSub      middleware.MessageMiddlewareQueue
	dataSub     middleware.MessageMiddlewareQueue
	nextStepPub middleware.MessageMiddlewareExchange
}

func NewTopKWorker(topK, total int, id string, rbConfig middleware.RabbitConfig) (*TopKWorker, error) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM)

	log := logger.GetLoggerWithPrefix("[TOPK WORKER" + id + "]")

	log.Infof("Initializing TopKWorker with K=%d", topK)

	rabbitConn, err := middleware.NewRabbitConnection(&rbConfig)

	if err != nil {
		log.Errorf("Failed to connect to RabbitMQ: %v", err)
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	return &TopKWorker{
		id:      id,
		sigChan: sigChan,
		topK:    topK,
		rbConn:  rabbitConn,
		errChan: make(chan middleware.MessageMiddlewareError),
		topKMap: make(map[string]map[string]*Toper[TopKRegister]),
		log:     log,
	}, nil
}

func (t *TopKWorker) createTopKExchangeHandler() error {
	eofPubRouteKey := fmt.Sprintf("eof.topk.%s", t.id)
	eofPub, err := createExchangeHandler(t.rbConn, eofPubRouteKey, middleware.EXCHANGE_TYPE_TOPIC)

	if err != nil {
		return fmt.Errorf("failed to create next stage publishing exchange: %w", err)
	}

	eofSub, err := prepareEofQueue(t.rbConn, t.id)
	if err != nil {
		return fmt.Errorf("error preparing EOF queue for eof.topk: %v", err)
	}

	dataSub, err := prepareDataQueue(t.rbConn, t.id)

	if err != nil {
		return fmt.Errorf("error preparing data queue for topk: %v", err)
	}

	nextStepPub, err := createExchangeHandler(t.rbConn, "", middleware.EXCHANGE_TYPE_DIRECT)

	if err != nil {
		return fmt.Errorf("failed to create next stage publishing exchange: %w", err)
	}

	t.exchHandler = &TopKMiddlewareHandler{
		eofPub:      *eofPub,
		eofSub:      *eofSub,
		dataSub:     *dataSub,
		nextStepPub: *nextStepPub,
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

	t.exchHandler.dataSub.StartConsuming(t.processDataMessage, t.errChan)

	return nil
}

func (t *TopKWorker) processDataMessage(message amqp.Delivery) error {
	defer answerMessage(ACK, message)

	msg, err := middleware.NewMessageGroupedFromBytes(message.Body)

	if err != nil {
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	isEof := msg.IsEof

	if isEof {
		go t.initiateEofCoordination(msg)
		answerMessage(ACK, message)
		return nil
	}

	t.mergeMessages(*msg)
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

func (t *TopKWorker) initiateEofCoordination(msg *middleware.MessageGrouped) {
	clientId := msg.ClientId
	t.log.Infof("Received EOF message for client %s", clientId)

	totalOfEofs := t.total - 1

	if totalOfEofs == 0 {
		t.log.Infof("No EOF coordination needed for %s", msg.DataType)
	} else {
		t.log.Infof("Coordinating EOF for %s", msg.DataType)
	}

	for i := 0; i < totalOfEofs; i++ {
		t.log.Warningf("BEFORE %d %s", i, msg.DataType)
		<-t.eofChan
		t.log.Warningf("AFTER %d %s", i, msg.DataType)
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
	t.log.Info("Shutting down TopKWorker...")
}

func (t *TopKWorker) handleSignal() {
	<-t.sigChan
	t.Shutdown()
}
