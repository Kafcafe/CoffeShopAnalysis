package join

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
	ACTIVITY                  = 0
)

type ClientId = string
type DataType = string

type JoinGenericWorker struct {
	log               *logging.Logger
	middlewareHandler *middleware.MiddlewareHandler
	sigChan           chan os.Signal
	isRunning         bool

	conf               JoinWorkerConfig
	middlewareHandlers JoinMiddlewareHandlers
	errChan            chan middleware.MessageMiddlewareError

	mutex           sync.Mutex
	middlewareMutex sync.Mutex
	clientsStats    map[string]*ClientStatsJoin

	sideTable         map[ClientId][]string
	mainTable         map[ClientId][]string
	sideTableReceived map[ClientId]chan int

	resultsChans  map[ClientId]map[DataType]chan middleware.MessageResultsResponse
	watchMesh     *watch_mesh.WatchMesh
	atomicWritter *atomicwritter.AtomicWriter
	crasher       *crasher.Crasher
}

// handleSignal listens for SIGTERM signal and triggers shutdown.
func (j *JoinGenericWorker) handleSignal() {
	<-j.sigChan
	j.log.Info("Handling signal")
	j.Shutdown()
}

func NewJoinWorker(
	rabbitConf middleware.RabbitConfig,
	config JoinWorkerConfig,
	watchMeshConfig watch_mesh.WatchMeshConfig,
) (*JoinGenericWorker, error) {
	log := logger.GetLoggerWithPrefix("[JOINER-" + config.id + "]")

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
	log.Infof("Using atomic writer path: %s", path)
	return &JoinGenericWorker{
		log:               log,
		middlewareHandler: middlewareHandler,
		sigChan:           sigChan,
		isRunning:         true,

		conf:    config,
		errChan: make(chan middleware.MessageMiddlewareError, ERROR_CHANNEL_BUFFER_SIZE),

		mutex:           sync.Mutex{},
		middlewareMutex: sync.Mutex{},
		clientsStats:    make(map[string]*ClientStatsJoin),

		sideTable:         map[string][]string{},
		mainTable:         map[string][]string{},
		sideTableReceived: make(map[ClientId]chan int),

		resultsChans:  make(map[ClientId]map[DataType]chan middleware.MessageResultsResponse),
		watchMesh:     watch_mesh.NewWatchMesh(watchMeshConfig),
		atomicWritter: atomicwritter.NewAtomicWriter(path),
		crasher:       crasher.NewCrasher(config.crasherEnabled),
	}, nil
}

func (j *JoinGenericWorker) joinWithPayload(message amqp.Delivery) error {
	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	clientStats := j.getClientStats(msg.ClientId)

	if clientStats.WasMessageProcessed(message.MessageId) {
		j.log.Infof("Message %s already processed", message.MessageId)
		answerMessage(ACK, message)
		return nil
	}

	j.crasher.ThrowDiceAndForceExit("before processing message")

	j.mutex.Lock()
	_, exists := j.sideTable[msg.ClientId]
	if _, ok := j.sideTableReceived[msg.ClientId]; !ok {
		j.sideTableReceived[msg.ClientId] = make(chan int, SINGLE_ITEM_BUFFER_LEN)
	}
	j.mutex.Unlock()
	if !exists && !clientStats.sideTableIsReady {
		j.log.Warning("Side table not ready!!!! Waiting...")
		<-j.sideTableReceived[msg.ClientId]
		j.log.Warning("Side table ready!!! Continuing...")
	}

	msg.QueryId = j.conf.queryId

	if msg.IsEof {
		go j.handleEofMessage(message, *msg)
		return nil
	}

	j.mutex.Lock()
	partialUpdate := j.conf.messageCallbackUpdateSideTable(j.sideTable[msg.ClientId], msg.Payload)
	j.sideTable[msg.ClientId] = partialUpdate
	j.crasher.ThrowDiceAndForceExit("after processing message - before stats update")
	clientStats.Add(msg.DataType, message.MessageId, true, false, "users", partialUpdate)
	j.crasher.ThrowDiceAndForceExit("after processing message - between stats update and save worker state")
	if err := j.Dump(clientStats, msg.ClientId); err != nil {
		j.mutex.Unlock()
		answerMessage(NACK_REQUEUE, message)
		return err
	}
	j.mutex.Unlock()

	answerMessage(ACK, message)

	return nil
}

func (j *JoinGenericWorker) joinWithSideTable(message amqp.Delivery) error {
	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	clientStats := j.getClientStats(msg.ClientId)

	if clientStats.WasMessageProcessed(message.MessageId) {
		j.log.Infof("Message %s already processed", message.MessageId)
		answerMessage(ACK, message)
		return nil
	}

	j.crasher.ThrowDiceAndForceExit("before processing message")

	msg.QueryId = j.conf.queryId

	if msg.IsEof {
		go j.handleEofMessage(message, *msg)
		return nil
	}

	flattenedPayload := flattenPayload(msg.GroupedPayload)
	j.mutex.Lock()
	j.mainTable[msg.ClientId] = append(j.mainTable[msg.ClientId], flattenedPayload...)
	j.crasher.ThrowDiceAndForceExit("after processing message - before stats update")
	clientStats.Add(msg.DataType, message.MessageId, true, false, "main", flattenedPayload)
	j.crasher.ThrowDiceAndForceExit("after processing message - between stats update and save worker state")
	if err := j.Dump(clientStats, msg.ClientId); err != nil {
		j.mutex.Unlock()
		answerMessage(NACK_REQUEUE, message)
		return err
	}
	j.mutex.Unlock()

	j.crasher.ThrowDiceAndForceExit("after processing message - before ack")
	answerMessage(ACK, message)
	return nil
}

func (j *JoinGenericWorker) saveSideTable(message amqp.Delivery) error {

	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		answerMessage(NACK_DISCARD, message)
		return err
	}

	clientStats := j.getClientStats(msg.ClientId)

	j.crasher.ThrowDiceAndForceExit("before processing message")

	if msg.IsEof {
		j.log.Infof("Received EOF for %s. Ready to Join.", j.conf.ofType)
		answerMessage(ACK, message)
		j.mutex.Lock()
		if _, exists := j.sideTableReceived[msg.ClientId]; !exists {
			j.sideTableReceived[msg.ClientId] = make(chan int, SINGLE_ITEM_BUFFER_LEN)
		}
		j.clientsStats[msg.ClientId].MarkSideTableAsReady()
		j.log.Infof("Marked side table as ready for client %s", msg.ClientId)
		if err := j.Dump(clientStats, msg.ClientId); err != nil {
			j.mutex.Unlock()
			answerMessage(NACK_REQUEUE, message)
			return err
		}
		j.mutex.Unlock()
		j.sideTableReceived[msg.ClientId] <- ACTIVITY
		return nil
	}

	j.mutex.Lock()
	_, exists := j.sideTable[msg.ClientId]
	if !exists {
		j.sideTable[msg.ClientId] = []string{}
	}
	j.sideTable[msg.ClientId] = append(j.sideTable[msg.ClientId], msg.Payload...)
	j.crasher.ThrowDiceAndForceExit("after processing message - before stats update")
	clientStats.Add(msg.DataType, message.MessageId, true, false, "side", msg.Payload)
	j.crasher.ThrowDiceAndForceExit("after processing message - between stats update and save worker state")
	if err := j.Dump(clientStats, msg.ClientId); err != nil {
		j.mutex.Unlock()
		answerMessage(NACK_REQUEUE, message)
		return err
	}
	j.mutex.Unlock()

	j.log.Infof("Side table size for client %s: %d", msg.ClientId, len(j.sideTable[msg.ClientId]))
	j.crasher.ThrowDiceAndForceExit("after processing message - before ack")
	answerMessage(ACK, message)
	return nil
}

func (j *JoinGenericWorker) getClientStats(clientId string) *ClientStatsJoin {
	if _, exists := j.clientsStats[clientId]; !exists {
		j.clientsStats[clientId] = NewClientStatsJoin(middleware.DEFAULT_CACHE_CAPACITY)
	}
	return j.clientsStats[clientId]
}

func (j *JoinGenericWorker) Run() error {
	go j.handleSignal()

	j.watchMesh.Start()

	j.crasher.ThrowDiceAndForceExit("before recover")
	if err := j.Recover(); err != nil {
		return fmt.Errorf("failed to recover state: %v", err)
	}
	j.crasher.ThrowDiceAndForceExit("after recover")

	err := j.createExchangeHandlers()
	if err != nil {
		return fmt.Errorf("failed to create exchange handlers: %v", err)
	}

	j.log.Info("Waiting to receive side table...")
	j.middlewareHandlers.sideTableSub.StartConsuming(j.saveSideTable, j.errChan)

	if !j.isRunning {
		return nil
	}

	if j.conf.ofType == JOIN_USERS_TYPE {
		j.middlewareHandlers.privateQueueSub.StartConsuming(j.joinWithPayload, j.errChan)
	} else {
		j.middlewareHandlers.privateQueueSub.StartConsuming(j.joinWithSideTable, j.errChan)
	}
	j.middlewareHandlers.prevStageSub.StartConsuming(j.dispatchMessage, j.errChan)
	j.middlewareHandlers.broadcastResultsRequestSub.StartConsuming(j.sendResultsRequest, j.errChan)

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
	j.errChan <- middleware.MessageMiddlewareSuccess

	j.middlewareHandlers.Shutdown()
	j.middlewareHandler.Close()
	values, err := j.atomicWritter.CleanAll()
	if err != nil {
		j.log.Warningf("Failed to count lines during shutdown: %v", err)
	} else {
		j.log.Infof("Cleaned stored data during shutdown. Total lines removed: %d", values)
	}
	j.log.Info("Shutdown complete")
}
