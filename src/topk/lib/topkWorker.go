package topk

import (
	"common/logger"
	"common/middleware"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

type TopKWorker struct {
	id          string
	sigChan     chan os.Signal
	topK        int
	rbConn      *middleware.RabbitConnection
	exchHandler *TopKMiddlewareHandler
}

type TopKMiddlewareHandler struct {
	eofPub      middleware.MessageMiddlewareExchange
	eofSub      middleware.MessageMiddlewareQueue
	dataSub     middleware.MessageMiddlewareQueue
	nextStepPub middleware.MessageMiddlewareExchange
}

func NewTopKWorker(topK int, id string, rbConfig middleware.RabbitConfig) (*TopKWorker, error) {
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

	return nil
}

func (t *TopKWorker) Shutdown() {

}

func (t *TopKWorker) handleSignal() {
	<-t.sigChan
	t.Shutdown()
}
