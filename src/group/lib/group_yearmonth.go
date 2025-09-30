package group

import (
	"common/logger"
	"common/middleware"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/op/go-logging"
)

type YearmonthExchangeHandlers struct {
	// transactionsSubscribing                 middleware.MessageMiddlewareExchange
	// transactionsYearFilteredPublishing      middleware.MessageMiddlewareExchange
	// transactionsItemsYearFilteredPublishing middleware.MessageMiddlewareExchange
	eofPublishing   middleware.MessageMiddlewareExchange
	eofSubscription middleware.MessageMiddlewareQueue
}

type GroupByYearmonthWorker struct {
	log                       *logging.Logger
	rabbitConn                *middleware.RabbitConnection
	sigChan                   chan os.Signal
	isRunning                 bool
	exchangeHandlers          YearmonthExchangeHandlers
	errChan                   chan middleware.MessageMiddlewareError
	id                        string
	groupByCount              int
	currentMessageProcessing  middleware.Message
	mutex                     sync.Mutex
	eofChan                   chan int
	eofIntercommunicationChan chan int
}

func NewGroupByYearmonthWorker(rabbitConf middleware.RabbitConfig, groupById string, groupByCount int) (*GroupByYearmonthWorker, error) {
	log := logger.GetLoggerWithPrefix("[GROUP-YM]")

	log.Infof("Establishing connection with RabbitMQ on address %s:%d", rabbitConf.Host, rabbitConf.Port)

	rabbitConn, err := middleware.NewRabbitConnection(&rabbitConf)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	log.Info("Connection with RabbitMQ successfully established")

	sigChan := make(chan os.Signal, SINGLE_ITEM_BUFFER_LEN)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	return &GroupByYearmonthWorker{
		log:                       log,
		rabbitConn:                rabbitConn,
		sigChan:                   sigChan,
		isRunning:                 true,
		errChan:                   make(chan middleware.MessageMiddlewareError, ERROR_CHANNEL_BUFFER_SIZE),
		id:                        groupById,
		groupByCount:              groupByCount,
		mutex:                     sync.Mutex{},
		eofChan:                   make(chan int, SINGLE_ITEM_BUFFER_LEN),
		eofIntercommunicationChan: make(chan int, SINGLE_ITEM_BUFFER_LEN),
	}, nil
}

func (g *GroupByYearmonthWorker) Run() error {
	return nil
}
