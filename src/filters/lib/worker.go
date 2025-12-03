package filters

import (
	atomicwritter "common/atomic_writter"
	"common/crasher"
	"common/logger"
	"common/middleware"
	"common/watch_mesh"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ERROR_CHANNEL_BUFFER_SIZE = 20
	SINGLE_ITEM_BUFFER_LEN    = 1
)

type ClientId = string
type DataType = string

type FilterGenericWorker struct {
	log               *logging.Logger
	middlewareHandler *middleware.MiddlewareHandler
	sigChan           chan os.Signal
	isRunning         bool

	filter             Filter
	conf               FilterConfig
	middlewareHandlers MiddlewareHandlers
	errChan            chan middleware.MessageMiddlewareError
	// new eof
	clientsStatsMutex sync.Mutex
	clientsStats      map[ClientId]*middleware.ClientStats
	resultsChans      map[ClientId]map[DataType]chan middleware.MessageResultsResponse
	watchMesh         *watch_mesh.WatchMesh
	atomicWritter     *atomicwritter.AtomicWriter
	crasher           *crasher.Crasher
}

// handleSignal listens for SIGTERM signal and triggers shutdown.
func (f *FilterGenericWorker) handleSignal() {
	<-f.sigChan
	f.log.Info("Handling signal")
	f.Shutdown()
}

func NewFilterGenericWorker(
	rabbitConf middleware.RabbitConfig,
	config FilterConfig,
	watchMeshConfig watch_mesh.WatchMeshConfig,
) (*FilterGenericWorker, error) {
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

	prefix := fmt.Sprintf("%s_%d", config.ofType, config.idNum)
	path := fmt.Sprintf("processed_data/%s", prefix)

	return &FilterGenericWorker{
		log:               log,
		middlewareHandler: middlewareHandler,
		sigChan:           sigChan,
		isRunning:         true,
		filter:            *NewFilter(),
		conf:              config,
		errChan:           make(chan middleware.MessageMiddlewareError, ERROR_CHANNEL_BUFFER_SIZE),
		clientsStatsMutex: sync.Mutex{},
		clientsStats:      make(map[ClientId]*middleware.ClientStats),
		resultsChans:      make(map[ClientId]map[DataType]chan middleware.MessageResultsResponse),
		watchMesh:         watch_mesh.NewWatchMesh(watchMeshConfig),
		atomicWritter:     atomicwritter.NewAtomicWriter(path),
		crasher:           crasher.NewCrasher(config.crasherEnabled),
	}, nil
}

func (f *FilterGenericWorker) filterMessage(message amqp.Delivery) error {
	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	if f.conf.ofType == FILTER_TYPE_AMOUNT {
		msg.QueryId = 1
	}

	f.crasher.ThrowDiceAndForceExit("before processing message")

	if msg.IsEof {
		go f.handleEofMessage(message, *msg)
		return nil
	}

	filteredBatch := f.conf.messageCallback(&f.filter, msg.Payload)

	if len(filteredBatch) == 0 {
		f.crasher.ThrowDiceAndForceExit("after processing message - before stats update")
		f.getClientStats(msg.ClientId).Add(msg.DataType, message.MessageId, true, false)
		f.crasher.ThrowDiceAndForceExit("after processing message - before save worker state")
		f.saveWorkerState(msg.ClientId)
		f.crasher.ThrowDiceAndForceExit("after processing message - before ack")
		answerMessage(ACK, message)
		return nil
	}

	response := middleware.NewMessageWithPayload(msg.DataType, msg.ClientId, filteredBatch, false, msg.QueryId)
	err = f.sendNextStage(*response)
	if err != nil {
		f.log.Errorf("Failed to send message to next stage: %v", err)
		answerMessage(NACK_REQUEUE, message)
		return err
	}

	f.crasher.ThrowDiceAndForceExit("after processing message - before stats update")

	f.getClientStats(msg.ClientId).Add(msg.DataType, message.MessageId, true, true)
	f.crasher.ThrowDiceAndForceExit("after processing message - between stats update and save worker state")
	f.saveWorkerState(msg.ClientId)
	f.crasher.ThrowDiceAndForceExit("after processing message - before ack")

	answerMessage(ACK, message)
	// f.log.Infof("Filtered message and sent to next stage")
	return nil
}

func (f *FilterGenericWorker) getClientStats(clientId ClientId) *middleware.ClientStats {
	if _, exists := f.clientsStats[clientId]; !exists {
		f.clientsStats[clientId] = middleware.NewClientStats(0)
	}
	return f.clientsStats[clientId]
}

func (f *FilterGenericWorker) ensureResultsChanExists(clientId ClientId, dataType DataType) {
	if _, exists := f.resultsChans[clientId]; !exists {
		f.resultsChans[clientId] = make(map[DataType]chan middleware.MessageResultsResponse)
	}
	if _, exists := f.resultsChans[clientId][dataType]; !exists {
		f.resultsChans[clientId][dataType] = make(chan middleware.MessageResultsResponse)
	}
}

func (f *FilterGenericWorker) Run() error {
	defer f.Shutdown()
	go f.handleSignal()

	f.watchMesh.Start()

	f.crasher.ThrowDiceAndForceExit("before createExchangeHandlers")
	err := f.createExchangeHandlers()
	if err != nil {
		return fmt.Errorf("failed to create exchange handlers: %v", err)
	}

	f.crasher.ThrowDiceAndForceExit("before recover")
	f.recover()
	f.crasher.ThrowDiceAndForceExit("after recover")

	f.middlewareHandlers.prevStageSub.StartConsuming(f.filterMessage, f.errChan)
	f.middlewareHandlers.broadcastResultsRequestSub.StartConsuming(f.sendResultsRequest, f.errChan)

	for err := range f.errChan {
		if err != middleware.MessageMiddlewareSuccess {
			f.log.Errorf("Error found while filtering message of type: %v", err)
		}

		if !f.isRunning {
			f.log.Info("Inside error loop: breaking")
			break
		}
	}

	f.log.Info("Finished filtering")
	return nil
}

// Shutdown gracefully stops the acceptor, closing the listener and current client.
func (f *FilterGenericWorker) Shutdown() {
	f.isRunning = false
	f.errChan <- middleware.MessageMiddlewareSuccess
	f.middlewareHandler.Close()

	f.middlewareHandlers.prevStageSub.Close()
	f.middlewareHandlers.broadcastResultsRequestPub.Close()
	f.middlewareHandlers.broadcastResultsRequestSub.Close()
	count, err := f.atomicWritter.CleanAll()
	if err != nil {
		f.log.Errorf("Failed to clean all files: %v", err)
	}
	f.log.Infof("Cleaned %d files", count)
	for _, nextStagePub := range f.middlewareHandlers.nextStagePubs {
		nextStagePub.Close()
	}

	f.log.Info("Shutdown complete")
}
