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
	"time"

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
	group           structures.GrouperPerClient[structures.AllowedGroup]
	// new eof
	clientsStats  map[ClientId]*middleware.ClientStats
	resultsChans  map[ClientId]map[DataType]chan middleware.MessageResultsResponse
	watchMesh     *watch_mesh.WatchMesh
	atomicWritter *atomicwritter.AtomicWriter
	cache         *middleware.Cache
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
		group:           structures.NewGrouperPerClient[structures.AllowedGroup](),

		clientsStats:  make(map[ClientId]*middleware.ClientStats),
		resultsChans:  make(map[ClientId]map[DataType]chan middleware.MessageResultsResponse),
		watchMesh:     watch_mesh.NewWatchMesh(watchMeshConfig),
		atomicWritter: atomicwritter.NewAtomicWriter(path),
		cache:         middleware.NewCache(middleware.DEFAULT_CACHE_CAPACITY),
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
		g.clientsStats[clientId] = middleware.NewClientStats()
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
	g.log.Infof("Processing message %s", message.MessageId)
	if g.cache.Contains(message.MessageId) {
		g.log.Infof("Message %s already processed", message.MessageId)
		answerMessage(ACK, message)
		return nil
	}

	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	if msg.IsEof {
		go g.handleEofMessage(message, *msg)
		return nil
	}

	g.mutex.Lock()
	g.group.Add(msg.ClientId, msg.Payload, g.conf.factory)
	g.getClientStats(msg.ClientId).Add(msg.DataType, true, false)
	dataType := msg.DataType
	if err := g.dumpData(msg, message.MessageId, dataType); err != nil {
		g.mutex.Unlock()
		answerMessage(NACK_REQUEUE, message)
		return err
	}
	g.mutex.Unlock()
	g.cache.Add(message.MessageId)

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

func (g *GroupByGenericWorker) broadcastAndWaitForResults(requestBytes []byte, clientId ClientId, dataType DataType, expectedEmitted int) (processed, emitted int, results structures.AllowedGroup, timeout bool) {
	for retriesCount := 0; retriesCount < middleware.MAX_EOF_RETRIES; retriesCount++ {
		processed = 0
		emitted = 0
		results = g.conf.factory()
		timeout = false
		timeoutDuration := time.Second * time.Duration(middleware.RESPONSE_TIMEOUT_SEC*(retriesCount+1))
		if sendErr := g.exchangeHandlers.broadcastResultsRequestPub.Send(requestBytes); sendErr != middleware.MessageMiddlewareSuccess {
			g.log.Errorf("Failed to send results request message to broadcast exchange: %v", sendErr)
			break
		}
		g.log.Infof("Sent results request message to broadcast exchange for client %s and dataType %s. Attempt %d/%d", clientId, dataType, retriesCount+1, middleware.MAX_EOF_RETRIES)
		for !timeout && processed < expectedEmitted {
			select {
			case msg := <-g.resultsChans[clientId][dataType]:
				processed += msg.Processed
				emitted += msg.Emitted
				if msg.GroupedPayload != nil {
					results.AddMapString(msg.GroupedPayload)
				}
			case <-time.After(timeoutDuration):
				g.log.Warningf("Timeout waiting for results response for client %s and datatype %s after %d seconds. Processed %d/%d", clientId, dataType, middleware.RESPONSE_TIMEOUT_SEC, processed, expectedEmitted)
				timeout = true
			}
		}
		if !timeout {
			break
		}
	}
	return processed, emitted, results, timeout
}

func (g *GroupByGenericWorker) Run() error {
	defer g.Shutdown()
	go g.handleSignal()

	g.watchMesh.Start()

	err := g.createExchangeHandlers()
	if err != nil {
		return fmt.Errorf("failed to create exchange handlers: %v", err)
	}

	g.recover()

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
	if err := g.atomicWritter.CleanAll(); err != nil {
		g.log.Errorf("Error during atomic writter cleanup: %v", err)
	}
	g.log.Info("Shutdown complete")
}
