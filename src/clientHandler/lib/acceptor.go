package clientHandler

import (
	logger "common/logger"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/op/go-logging"
)

const (
	TRANSPORT_LAYER_PROTO  = "tcp"
	SINGLE_ITEM_BUFFER_LEN = 1
)

type Acceptor struct {
	listener   net.Listener
	isRunning  bool
	currClient *ClientHandler
	sigChan    chan os.Signal
	log        *logging.Logger
}

// handleSignal listens for SIGTERM signal and triggers shutdown.
func (a *Acceptor) handleSignal() {
	<-a.sigChan
	a.Shutdown()
}

// NewAcceptor creates a new Acceptor instance with the given server configuration.
// It starts listening on the specified IP and port, and sets up signal handling for graceful shutdown.
// Parameters:
//
//	serverConfigs: the server configuration containing IP and port
//
// Returns the Acceptor instance or an error if listening fails.
func NewAcceptor(clientHandlerConfig *ClientHandlerConfig) (*Acceptor, error) {
	log := logger.GetLoggerWithPrefix("[ACCEPTOR]")

	listenAddr := fmt.Sprintf("%s:%d", clientHandlerConfig.Ip, clientHandlerConfig.Port)

	listener, err := net.Listen(TRANSPORT_LAYER_PROTO, listenAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to start server: %v", err)
	}

	log.Infof("Server listening on %s", listenAddr)

	acceptor := &Acceptor{
		listener:   listener,
		isRunning:  true,
		currClient: nil,
		sigChan:    make(chan os.Signal, SINGLE_ITEM_BUFFER_LEN),
		log:        log,
	}

	// Set up signal notification for graceful shutdown
	signal.Notify(acceptor.sigChan, syscall.SIGTERM, syscall.SIGINT)

	return acceptor, nil
}

// Run starts the acceptor loop to accept and handle client connections.
// It runs until shutdown is signaled, accepting one connection at a time.
// Returns an error if accepting fails.
func (a *Acceptor) Run() error {
	a.log.Info("Running and ready to accept connections")
	defer a.Shutdown()
	go a.handleSignal()

	for a.isRunning {
		a.log.Info("Waiting for a new client connection...")

		conn, err := a.listener.Accept()
		if err != nil {
			return fmt.Errorf("Failed to accept connection: %v", err)
		}

		a.log.Infof("Accepted connection from %s", conn.RemoteAddr().String())
		a.currClient = NewClientHandler(conn)
		a.log.Infof("Assigned client id %s with short form %s", a.currClient.ClientId, a.currClient.ClientIdShort)

		err = a.currClient.Handle()
		if err != nil {
			a.log.Errorf("Error handling client connection: %v", err)
		}

		a.log.Info("Closing client connection, conection finished successfully")
		a.currClient.Shutdown()
	}

	return nil
}

// Shutdown gracefully stops the acceptor, closing the listener and current client.
func (a *Acceptor) Shutdown() {
	a.isRunning = false

	if a.listener != nil {
		a.listener.Close()
	}

	if a.currClient != nil {
		a.currClient.Shutdown()
	}

	a.log.Info("Shutdown complete")
}
