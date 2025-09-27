package clientHandler

import (
	"fmt"
	"net"
	"os"
	"os/signal"

	"github.com/op/go-logging"
)

const transport = "tcp"

var log = logging.MustGetLogger("log")

type Acceptor struct {
	listener   net.Listener
	isRunning  bool
	currClient *ClientHandler
	sigChan    chan os.Signal
}

func (a *Acceptor) handleSignal() {
	<-a.sigChan
	a.Shutdown()
}

func NewAcceptor(serverConfigs *ServerConfig) (*Acceptor, error) {

	listenAddr := fmt.Sprintf("%s:%d", serverConfigs.IP, serverConfigs.Port)
	listener, err := net.Listen(transport, listenAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to start server: %v", err)
	}
	log.Infof("Server listening on %s", listenAddr)
	acceptor := &Acceptor{
		listener:   listener,
		isRunning:  true,
		currClient: nil,
		sigChan:    make(chan os.Signal, 1),
	}

	signal.Notify(acceptor.sigChan, os.Interrupt)
	return acceptor, nil

}

func (a *Acceptor) Run() error {
	// Implement the logic to accept client connections and handle them
	log.Info("Acceptor is running and ready to accept connections")
	defer a.Shutdown()
	go a.handleSignal()
	for a.isRunning {
		log.Info("Waiting for a new client connection...")
		conn, err := a.listener.Accept()
		if err != nil {
			log.Errorf("Failed to accept connection: %v", err)
			return err
		}
		log.Infof("Accepted connection from %s", conn.RemoteAddr().String())
		a.currClient = NewClientHandler(conn)
		err = a.currClient.Handle()

		if err != nil {
			log.Errorf("Error handling client connection: %v", err)
		}

		log.Info("Closing client connection, conection finished successfully")
		a.currClient.Shutdown()

	}

	return nil
}

func (a *Acceptor) Shutdown() {
	a.isRunning = false
	if a.listener != nil {
		a.listener.Close()
	}
	if a.currClient != nil {
		a.currClient.Shutdown()
	}

	log.Info("Acceptor shutdown complete")
}
