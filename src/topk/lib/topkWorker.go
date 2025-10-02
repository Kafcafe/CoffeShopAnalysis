package topk

import (
	"os"
	"os/signal"
	"syscall"
)

type TopKWorker struct {
	sigChan chan os.Signal
}

func NewTopKWorker() *TopKWorker {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM)
	return &TopKWorker{
		sigChan: sigChan,
	}
}

func (t *TopKWorker) Run() error {
	go t.handleSignal()
	return nil
}

func (t *TopKWorker) Shutdown() {

}

func (t *TopKWorker) handleSignal() {
	<-t.sigChan
	t.Shutdown()
}
