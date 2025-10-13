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

type MiddlewareHandlers struct {
	prevStageSub middleware.MessageMiddlewareQueue
	nextStagePub middleware.MessageMiddlewareExchange
	eofPub       middleware.MessageMiddlewareExchange
	eofSub       middleware.MessageMiddlewareQueue
}

type GroupByGenericWorker struct {
	log               *logging.Logger
	middlewareHandler *middleware.MiddlewareHandler
	sigChan           chan os.Signal
	isRunning         bool

	exchangeHandlers MiddlewareHandlers
	errChan          chan middleware.MessageMiddlewareError

	conf GroupByConfig

	currentMessageProcessing  middleware.Message
	mutex                     sync.Mutex
	eofChan                   chan int
	eofIntercommunicationChan chan structures.AllowedGroup
	// groupedPerClient          structures.GroupedPerClient
	group structures.GrouperPerClient[structures.AllowedGroup]
}

func NewGroupByGenericWorker(rabbitConf middleware.RabbitConfig, conf GroupByConfig) (*GroupByGenericWorker, error) {
	log := logger.GetLoggerWithPrefix("[GROUP-GEN]")

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

	return &GroupByGenericWorker{
		log:               log,
		middlewareHandler: middlewareHandler,
		sigChan:           sigChan,
		isRunning:         true,
		errChan:           make(chan middleware.MessageMiddlewareError, ERROR_CHANNEL_BUFFER_SIZE),
		conf:              conf,

		mutex:                     sync.Mutex{},
		eofChan:                   make(chan int, SINGLE_ITEM_BUFFER_LEN),
		eofIntercommunicationChan: make(chan structures.AllowedGroup, SINGLE_ITEM_BUFFER_LEN),
		group:                     structures.NewGrouperPerClient[structures.AllowedGroup](),
	}, nil
}

// handleSignal listens for SIGTERM signal and triggers shutdown.
func (g *GroupByGenericWorker) handleSignal() {
	<-g.sigChan
	g.log.Info("Handling signal")
	g.Shutdown()
}

func (g *GroupByGenericWorker) processInboundEof(message amqp.Delivery) error {
	msg, err := middleware.NewEofMessageGroupedFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}
	g.log.Warningf("processInboundEof %s groupBy%s", msg.DataType, g.conf.id)

	didSomebodyElseAcked := msg.Origin == g.conf.id && msg.IsAck && msg.ImmediateSource != g.conf.id
	if didSomebodyElseAcked {
		g.log.Infof("Somebody else acked for %s groupBy%s", msg.DataType, g.conf.id)
		partialGrouping := g.conf.factory()
		partialGrouping.FromMapString(msg.Payload)

		g.log.Infof("%v", partialGrouping)
		g.eofIntercommunicationChan <- partialGrouping
		answerMessage(ACK, message)
		return nil
	}

	isAckMine := msg.ImmediateSource == g.conf.id
	isAckForNotForMe := msg.IsAck && msg.Origin != g.conf.id
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

	msg.ImmediateSource = g.conf.id
	msg.IsAck = true

	g.mutex.Lock()
	msg.Payload = g.group.ToMapString(msg.ClientId)
	g.mutex.Unlock()

	msgBytes, err := msg.ToBytes()
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	answerMessage(ACK, message)
	g.exchangeHandlers.eofPub.Send(msgBytes)
	return nil
}

func (g *GroupByGenericWorker) initiateEofCoordination(originalMsg middleware.Message) {
	eofMsg := middleware.NewEofMessageGrouped(originalMsg.DataType, originalMsg.ClientId, g.conf.id, g.conf.id, false, nil, originalMsg.QueryId)
	msgBytes, err := eofMsg.ToBytes()
	if err != nil {
		g.log.Errorf("Failed to serialize message: %v", err)
	}

	g.exchangeHandlers.eofPub.Send(msgBytes)

	totalEofs := g.conf.count - 1

	if totalEofs == 0 {
		g.log.Infof("No EOF coordination needed for %s", originalMsg.DataType)
	} else {
		g.log.Infof("Coordinating EOF for %s", originalMsg.DataType)
	}

	g.log.Infof("Consolidating partial results for %s", originalMsg.DataType)

	g.mutex.Lock()
	currentGroup := g.group.Get(originalMsg.ClientId, g.conf.factory)
	g.group.Delete(originalMsg.ClientId)
	g.mutex.Unlock()

	for i := 0; i < totalEofs; i++ {
		g.log.Warningf("BEFORE %d %s", i, originalMsg.DataType)

		partialGrouping := <-g.eofIntercommunicationChan

		g.log.Infof("%v", partialGrouping)
		g.log.Infof("%v", currentGroup)

		currentGroup.Merge(partialGrouping)

		g.log.Infof("%v", currentGroup)
		g.log.Warningf("AFTER %d %s", i, originalMsg.DataType)
	}

	messageToSend := currentGroup.GetMessageToSend()
	emitted := 0
	for _, messages := range messageToSend {
		for key, records := range messages {
			singleYearMonthRecords := map[string][]string{key: records}
			response := middleware.NewMessageGrouped(originalMsg.DataType, originalMsg.ClientId, singleYearMonthRecords, false, originalMsg.QueryId)
			responseBytes, err := response.ToBytes()
			if err != nil {
				g.log.Errorf("%v", err)
			}

			g.log.Infof("Sent consolidated results for year-month top profit: %s", key)

			middleError := g.exchangeHandlers.nextStagePub.Send(responseBytes)
			emitted++
			if middleError != middleware.MessageMiddlewareSuccess {
				g.log.Errorf("problem while sending message to %s", g.conf.nextStagePub)
			}
		}
	}

	g.log.Infof("Final results grouped and consolidated")

	originalMsg.TotalEmitted = emitted
	eofMessageBytes, err := originalMsg.ToBytes()
	if err != nil {
		g.log.Errorf("%v", err)
	}
	middleError := g.exchangeHandlers.nextStagePub.Send(eofMessageBytes)
	if middleError != middleware.MessageMiddlewareSuccess {
		g.log.Errorf("problem while propagating EOF")
	}

	g.log.Warningf("Propagated EOF for %s to next pipeline stage. Total emitted: %d", originalMsg.DataType, originalMsg.TotalEmitted)
}

func (g *GroupByGenericWorker) groupByYearmonth(message amqp.Delivery) error {
	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	if msg.IsEof {
		go g.initiateEofCoordination(*msg)
		answerMessage(ACK, message) // REVISAR, se manda ACK antes de procesar eof
		return nil
	}

	if len(g.eofChan) > 0 {
		<-g.eofChan
	}

	g.mutex.Lock()
	g.currentMessageProcessing = *msg
	g.currentMessageProcessing.Payload = []string{}
	g.group.Add(msg.ClientId, msg.Payload, g.conf.factory)
	g.mutex.Unlock()

	answerMessage(ACK, message)

	g.eofChan <- THERE_IS_PREVIOUS_MESSAGE

	g.log.Info("Grouped message and sent groupByYearmonth batch")
	return nil
}

func (g *GroupByGenericWorker) createExchangeHandlers() error {
	// PREV STAGE SUB
	g.log.Infof("Creating exchange handler for previous stage subscription: %s", g.conf.prevStageSub)
	_, err := g.middlewareHandler.CreateDirectExchangeStandalone(g.conf.prevStageSub)
	if err != nil {
		return fmt.Errorf("error creating previous stage subscription exchange: %v", err)
	}

	// this name is just for identification purposes
	g.log.Infof("Creating queue handler for previous stage subscription: %s", g.conf.prevStageSub)
	prevStageQueueName := fmt.Sprintf("%s.%s", g.conf.prevStageSub, g.conf.ofType)
	prevStageSub, err := g.middlewareHandler.CreateQueue(prevStageQueueName)
	if err != nil {
		return fmt.Errorf("error creating queue for previous stage: %v", err)
	}

	g.log.Infof("Binding queue handler for previous stage subscription: %s", g.conf.prevStageSub)
	err = g.middlewareHandler.BindQueue(prevStageQueueName, middleware.EXCHANGE_NAME_DIRECT_TYPE, g.conf.prevStageSub)
	if err != nil {
		return fmt.Errorf("error binding queue for previous stage: %v", err)
	}

	// NEXT STAGE PUB
	g.log.Infof("Creating exchange handler for next stage publication: %s", g.conf.nextStagePub)
	nextStagePub, err := g.middlewareHandler.CreateDirectExchangeStandalone(g.conf.nextStagePub)
	if err != nil {
		return fmt.Errorf("error creating exchange handler for transactions.items: %v", err)
	}

	// EOF PUB/SUB
	g.log.Infof("Creating exchange and queue handlers for EOF coordination for groupBy %s", g.conf.ofType)
	eofPubRouteKey := fmt.Sprintf("eof.group.%s", g.conf.ofType)
	eofPub, err := g.middlewareHandler.CreateFanoutExchangeStandalone(eofPubRouteKey)
	if err != nil {
		return fmt.Errorf("error creating exchange handler for eof.group.yearmonth: %v", err)
	}

	// this name is just for identification purposes
	g.log.Infof("Creating queue handler for EOF coordination for groupBy %s", g.conf.ofType)
	eofSubQueueName := fmt.Sprintf("eof.group.%s.%s", g.conf.ofType, g.conf.id)
	eofSub, err := g.middlewareHandler.CreateQueue(eofSubQueueName)
	if err != nil {
		return fmt.Errorf("error creating queue for eof.group.yearmonth: %v", err)
	}

	g.log.Infof("Binding queue handler for EOF coordination for groupBy %s", g.conf.ofType)
	err = g.middlewareHandler.BindQueue(eofSubQueueName, eofPubRouteKey, "")
	if err != nil {
		return fmt.Errorf("error binding queue for eof.group.yearmonth: %v", err)
	}

	g.log.Info("Exchange handlers successfully created")
	g.exchangeHandlers = MiddlewareHandlers{
		prevStageSub: *prevStageSub,
		nextStagePub: *nextStagePub,
		eofPub:       *eofPub,
		eofSub:       *eofSub,
	}

	return nil
}

func (g *GroupByGenericWorker) Run() error {
	defer g.Shutdown()
	go g.handleSignal()

	err := g.createExchangeHandlers()
	if err != nil {
		return fmt.Errorf("failed to create exchange handlers: %v", err)
	}

	g.log.Infof("Starting to consume messages from %s", g.conf.prevStageSub)
	g.exchangeHandlers.prevStageSub.StartConsuming(g.groupByYearmonth, g.errChan)
	g.exchangeHandlers.eofSub.StartConsuming(g.processInboundEof, g.errChan)

	for err := range g.errChan {
		if err != middleware.MessageMiddlewareSuccess {
			g.log.Errorf("Error found while grouping by Yearmonth message of type: %v", err)
		}

		if !g.isRunning {
			g.log.Info("Inside error loop: breaking")
			break
		}
	}

	g.log.Info("Finished grouping")
	return nil
}

// Shutdown gracefully stops the acceptor, closing the listener and current client.
func (g *GroupByGenericWorker) Shutdown() {
	g.isRunning = false
	g.errChan <- middleware.MessageMiddlewareSuccess

	g.exchangeHandlers.prevStageSub.StopConsuming()
	g.exchangeHandlers.prevStageSub.Close()
	g.exchangeHandlers.nextStagePub.Close()
	g.exchangeHandlers.eofSub.StopConsuming()
	g.exchangeHandlers.eofSub.Close()
	g.exchangeHandlers.eofPub.Close()
	g.middlewareHandler.Close()

	g.log.Info("Shutdown complete")
}
