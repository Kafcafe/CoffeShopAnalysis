package group

import (
	atomicwritter "common/atomic_writter"
	"common/logger"
	"common/middleware"
	"common/watch_mesh"
	"fmt"
	"group/structures"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ERROR_CHANNEL_BUFFER_SIZE  = 20
	SINGLE_ITEM_BUFFER_LEN     = 1
	REQUEUE_PROBABILITY        = 0.1
	BATCH_SIZE_GROUPED_MESSAGE = 1000
)

type ClientId = string
type DataType = string

type GroupByGenericWorker struct {
	log               *logging.Logger
	middlewareHandler *middleware.MiddlewareHandler
	sigChan           chan os.Signal
	isRunning         bool

	exchangeHandlers MiddlewareHandlers
	errChan          chan middleware.MessageMiddlewareError

	conf GroupByConfig

	mutex           sync.Mutex
	middlewareMutex sync.Mutex
	group           *structures.GrouperPerClient[structures.AllowedGroup]
	// Dedicated channels for different operations to avoid AMQP concurrency issues
	sendChannel  *middleware.MiddlewareHandler
	queueChannel *middleware.MiddlewareHandler
	// Pool of dedicated channels for private queues to prevent frame collision
	privateChannels map[int]*middleware.MiddlewareHandler
	// new eof
	clientsStats  map[ClientId]*middleware.ClientStats
	resultsChans  map[ClientId]map[DataType]chan middleware.MessageResultsResponse
	watchMesh     *watch_mesh.WatchMesh
	atomicWritter *atomicwritter.AtomicWriter
}

func NewGroupByGenericWorker(
	rabbitConf middleware.RabbitConfig,
	conf GroupByConfig,
	watchMeshConfig watch_mesh.WatchMeshConfig,
) (*GroupByGenericWorker, error) {
	log := logger.GetLoggerWithPrefix("[GROUP-" + conf.id + "] ")

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

	// Create dedicated channels for different operations
	sendChannel, err := middleware.NewMiddlewareHandler(rabbitConn)
	if err != nil {
		return nil, fmt.Errorf("failed to create send channel: %v", err)
	}

	queueChannel, err := middleware.NewMiddlewareHandler(rabbitConn)
	if err != nil {
		return nil, fmt.Errorf("failed to create queue channel: %v", err)
	}

	// Create dedicated channels for each private queue to prevent AMQP frame collision
	privateChannels := make(map[int]*middleware.MiddlewareHandler)
	for i := 1; i <= conf.count; i++ {
		privateChannel, err := middleware.NewMiddlewareHandler(rabbitConn)
		if err != nil {
			return nil, fmt.Errorf("failed to create private channel %d: %v", i, err)
		}
		privateChannels[i] = privateChannel
	}

	sigChan := make(chan os.Signal, SINGLE_ITEM_BUFFER_LEN)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	prefix := fmt.Sprintf("%s_%d", conf.ofType, conf.idNum)
	path := fmt.Sprintf("processed_data/%s", prefix)
	return &GroupByGenericWorker{
		log:               log,
		middlewareHandler: middlewareHandler,
		sigChan:           sigChan,
		isRunning:         true,
		errChan:           make(chan middleware.MessageMiddlewareError, ERROR_CHANNEL_BUFFER_SIZE),
		conf:              conf,

		mutex:           sync.Mutex{},
		middlewareMutex: sync.Mutex{},
		group:           structures.NewGrouperPerClient(conf.factory),

		clientsStats:    make(map[ClientId]*middleware.ClientStats),
		resultsChans:    make(map[ClientId]map[DataType]chan middleware.MessageResultsResponse),
		watchMesh:       watch_mesh.NewWatchMesh(watchMeshConfig),
		atomicWritter:   atomicwritter.NewAtomicWriter(path),
		sendChannel:     sendChannel,
		queueChannel:    queueChannel,
		privateChannels: privateChannels,
	}, nil
}

// handleSignal listens for SIGTERM signal and triggers shutdown.
func (g *GroupByGenericWorker) handleSignal() {
	<-g.sigChan
	g.log.Info("Handling signal")
	g.Shutdown()
}

func (g *GroupByGenericWorker) getClientStats(clientId ClientId) *middleware.ClientStats {
	if _, exists := g.clientsStats[clientId]; !exists {
		g.clientsStats[clientId] = middleware.NewClientStats(middleware.DEFAULT_CACHE_CAPACITY)
	}
	return g.clientsStats[clientId]
}

func (g *GroupByGenericWorker) ensureResultsChanExists(clientId ClientId, dataType DataType) {
	if _, exists := g.resultsChans[clientId]; !exists {
		g.resultsChans[clientId] = make(map[DataType]chan middleware.MessageResultsResponse)
	}
	if _, exists := g.resultsChans[clientId][dataType]; !exists {
		g.resultsChans[clientId][dataType] = make(chan middleware.MessageResultsResponse)
	}
}

func (g *GroupByGenericWorker) groupMessage(message amqp.Delivery) error {
	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_REQUEUE, message)
		return err
	}
	clientStats := g.getClientStats(msg.ClientId)

	if clientStats.WasMessageProcessed(message.MessageId) {
		g.log.Infof("Message %s already processed", message.MessageId)
		answerMessage(ACK, message)
		return nil
	}

	g.watchMesh.TryCrash("before processing message")

	if msg.IsEof {
		go g.handleEofMessage(message, *msg)
		return nil
	}

	g.mutex.Lock()
	g.group.Add(msg.ClientId, msg.Payload)
	g.watchMesh.TryCrash("after processing message - before stats update")
	clientStats.Add(msg.DataType, message.MessageId, true, false)
	g.watchMesh.TryCrash("after processing message - between stats update and save worker state")
	dataType := msg.DataType
	if err := g.dumpData(msg, message.MessageId, dataType); err != nil {
		g.mutex.Unlock()
		answerMessage(NACK_REQUEUE, message)
		return err
	}
	g.mutex.Unlock()

	g.watchMesh.TryCrash("after processing message - before ack")
	answerMessage(ACK, message)
	return nil
}

func (g *GroupByGenericWorker) sendMessage(msgToSend middleware.Message) error {
	msgBytes, err := msgToSend.ToBytes()
	if err != nil {
		return err
	}
	return g.sendBytesNextStage(msgBytes)
}

func (g *GroupByGenericWorker) Run() error {
	defer g.Shutdown()
	go g.handleSignal()

	g.watchMesh.Start()

	g.watchMesh.TryCrash("before createExchangeHandlers")
	err := g.createExchangeHandlers()
	if err != nil {
		return fmt.Errorf("failed to create exchange handlers: %v", err)
	}

	g.watchMesh.TryCrash("before recover")
	g.recover()
	g.watchMesh.TryCrash("after recover")

	g.log.Infof("Starting to consume messages from %s", g.conf.prevStageSub)
	g.exchangeHandlers.privateQueueSub.StartConsuming(g.groupMessage, g.errChan)
	g.exchangeHandlers.prevStageSub.StartConsuming(g.dispatchMessage, g.errChan)
	g.exchangeHandlers.broadcastResultsRequestSub.StartConsuming(g.sendResultsRequest, g.errChan)

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
	g.exchangeHandlers.broadcastResultsRequestSub.StopConsuming()
	g.exchangeHandlers.broadcastResultsRequestSub.Close()
	g.middlewareHandler.Close()
	g.sendChannel.Close()
	g.queueChannel.Close()
	// Close all private channels
	for i, ch := range g.privateChannels {
		if err := ch.Close(); err != nil {
			g.log.Errorf("Error closing private channel %d: %v", i, err)
		}
	}
	count, err := g.atomicWritter.CleanAll()
	if err != nil {
		g.log.Errorf("Error during atomic writter cleanup: %v", err)
		return
	}
	g.log.Infof("Cleaned %d files during atomic writter cleanup", count)
	g.log.Info("Shutdown complete")
}
