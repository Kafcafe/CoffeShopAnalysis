package clientHandler

import (
	logger "common/logger"
	"common/middleware"
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
	config     AcceptorConfig
	listener   net.Listener
	isRunning  bool
	currClient *ClientHandler
	sigChan    chan os.Signal
	log        *logging.Logger
	rabbitConn *middleware.RabbitConnection
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
func NewAcceptor(acceptorConfig *AcceptorConfig) (*Acceptor, error) {
	log := logger.GetLoggerWithPrefix("[ACCEPTOR]")

	listenAddr := fmt.Sprintf("%s:%d", acceptorConfig.Ip, acceptorConfig.Port)

	listener, err := net.Listen(TRANSPORT_LAYER_PROTO, listenAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to start server: %v", err)
	}

	log.Infof("Establishing connection with RabbitMQ on address %s:%d",
		acceptorConfig.rabbitHost, acceptorConfig.rabbitPort)

	rabbitConn, err := middleware.NewRabbitConnection(acceptorConfig.rabbitUser,
		acceptorConfig.rabbitPassword,
		acceptorConfig.rabbitHost,
		acceptorConfig.rabbitPort)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	log.Info("Connection with RabbitMQ successfully established")
	log.Infof("Server listening on address %s", listenAddr)

	acceptor := &Acceptor{
		config:     *acceptorConfig,
		listener:   listener,
		isRunning:  true,
		currClient: nil,
		sigChan:    make(chan os.Signal, SINGLE_ITEM_BUFFER_LEN),
		log:        log,
		rabbitConn: rabbitConn,
	}

	// Set up signal notification for graceful shutdown
	signal.Notify(acceptor.sigChan, syscall.SIGTERM, syscall.SIGINT)

	return acceptor, nil
}

func (a *Acceptor) createExchangeHandler(routeKey string) (*middleware.MessageMiddlewareExchange, error) {
	middlewareHandler, err := middleware.NewMiddlewareHandler(a.rabbitConn)
	if err != nil {
		return nil, fmt.Errorf("fialed to create middleware handler: %w", middlewareHandler)
	}

	return middlewareHandler.CreateTopicExchange(routeKey)
}

type ExchangeHandlers struct {
	// General
	transactionsPublishing middleware.MessageMiddlewareExchange
	//transactionItemsPublishing Exchange

	// Side table Query 2
	// menuItemsByIdPublishing Exchange

	// Side table Query 4
	// storeNamesItemsByIdPublishing Exchange
	// birthdaysByUserIdPublishing   Exchange

	// Results
	//resultQuery1Subscription Exchange
	// resultQuery2Subscription Exchange
	// resultQuery3Subscription Exchange
	// resultQuery4Subscription Exchange
}

func (a *Acceptor) createExchangeHandlers(clientId ClientUuid) (*ExchangeHandlers, error) {
	transactionsRouteKey := fmt.Sprintf("transactions.%s", clientId.Full)
	transactionsPublishingHandler, err := a.createExchangeHandler(transactionsRouteKey)
	if err != nil {
		return nil, fmt.Errorf("Error creating exchange handler for transactions: %v", err)
	}

	return &ExchangeHandlers{
		transactionsPublishing: *transactionsPublishingHandler,
	}, nil
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
		newId := NewClientUuid()

		exchangeHandlers, err := a.createExchangeHandlers(newId)
		if err != nil {
			return fmt.Errorf("failed to create exchange handlers: %v", err)
		}

		a.currClient = NewClientHandler(conn, newId, *exchangeHandlers)

		a.log.Infof("Assigned client id %s with short form %s", a.currClient.ClientId, a.currClient.ClientId.Short)

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
