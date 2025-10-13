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
	config            AcceptorConfig
	listener          net.Listener
	isRunning         bool
	currClient        map[ClientUuid]*ClientHandler
	sigChan           chan os.Signal
	log               *logging.Logger
	middlewareHandler *middleware.MiddlewareHandler
	limitHandler      *ConnectionLimit
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

	rabbitConf := middleware.NewRabbitConfig(acceptorConfig.rabbitUser,
		acceptorConfig.rabbitPassword,
		acceptorConfig.rabbitHost,
		acceptorConfig.rabbitPort)

	rabbitConn, err := middleware.NewRabbitConnection(&rabbitConf)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	middlewareHandler, err := middleware.NewMiddlewareHandler(rabbitConn)
	if err != nil {
		return nil, fmt.Errorf("failed to create middleware handler: %v", err)
	}

	log.Info("Connection with RabbitMQ successfully established")
	log.Infof("Server listening on address %s", listenAddr)

	acceptor := &Acceptor{
		config:            *acceptorConfig,
		listener:          listener,
		isRunning:         true,
		currClient:        make(map[ClientUuid]*ClientHandler),
		sigChan:           make(chan os.Signal, SINGLE_ITEM_BUFFER_LEN),
		log:               log,
		middlewareHandler: middlewareHandler,
		limitHandler:      NewConnectionLimit(acceptorConfig.connectionLimit),
	}

	// Set up signal notification for graceful shutdown
	signal.Notify(acceptor.sigChan, syscall.SIGTERM, syscall.SIGINT)

	return acceptor, nil
}

func (a *Acceptor) createExchangeHandler(routeKey string, exchangeType string) (*middleware.MessageMiddlewareExchange, error) {
	if exchangeType == middleware.EXCHANGE_TYPE_DIRECT {
		return a.middlewareHandler.CreateDirectExchange(routeKey)
	}
	return a.middlewareHandler.CreateTopicExchange(routeKey)
}

type ExchangeHandlers struct {
	// General
	transactionsPublishing middleware.MessageMiddlewareExchange
	menuItemsPublishing    middleware.MessageMiddlewareExchange
	storePublishing        middleware.MessageMiddlewareExchange
	usersPublishing        middleware.MessageMiddlewareExchange
	//transactionItemsPublishing Exchange

	// Side table Query 2
	// menuItemsByIdPublishing Exchange

	// Side table Query 4
	// storeNamesItemsByIdPublishing Exchange
	// birthdaysByUserIdPublishing   Exchange

	// Results
	resultsQ1Subscription middleware.MessageMiddlewareExchange
	resultsQ2Subscription middleware.MessageMiddlewareExchange
	resultsQ3Subscription middleware.MessageMiddlewareExchange
	resultsQ4Subscription middleware.MessageMiddlewareExchange
}

func (a *Acceptor) createExchangeHandlers() (*ExchangeHandlers, error) {
	transactionsRouteKey := "transactions"
	transactionsPublishingHandler, err := a.createExchangeHandler(transactionsRouteKey, middleware.EXCHANGE_TYPE_DIRECT)
	if err != nil {
		return nil, fmt.Errorf("error creating exchange handler for transactions: %v", err)
	}

	resultsQ1SubscriptionRouteKey := "results.q1"
	resultsQ1SubscriptionHandler, err := a.createExchangeHandler(resultsQ1SubscriptionRouteKey, middleware.EXCHANGE_TYPE_DIRECT)
	if err != nil {
		return nil, fmt.Errorf("error creating exchange handler for transactions: %v", err)
	}

	resultsQ2SubscriptionRouteKey := "results.q2"
	resultsQ2SubscriptionHandler, err := a.createExchangeHandler(resultsQ2SubscriptionRouteKey, middleware.EXCHANGE_TYPE_DIRECT)
	if err != nil {
		return nil, fmt.Errorf("error creating exchange handler for transactions: %v", err)
	}

	resultsQ3SubscriptionRouteKey := "results.q3"
	resultsQ3SubscriptionHandler, err := a.createExchangeHandler(resultsQ3SubscriptionRouteKey, middleware.EXCHANGE_TYPE_DIRECT)
	if err != nil {
		return nil, fmt.Errorf("error creating exchange handler for transactions: %v", err)
	}

	resultsQ4SubscriptionRouteKey := "results.q4"
	resultsQ4SubscriptionHandler, err := a.createExchangeHandler(resultsQ4SubscriptionRouteKey, middleware.EXCHANGE_TYPE_DIRECT)
	if err != nil {
		return nil, fmt.Errorf("error creating exchange handler for transactions: %v", err)
	}

	menuItemsPublishingRouteKey := "transactions.items.menu.items"
	menuItemsPublishingHandler, err := a.middlewareHandler.CreateDirectExchangeStandalone(menuItemsPublishingRouteKey)
	if err != nil {
		return nil, fmt.Errorf("error creating exchange handler for menu_items: %v", err)
	}

	storePublishingRouteKey := "transactions.store"
	storePublishingHandler, err := a.middlewareHandler.CreateDirectExchangeStandalone(storePublishingRouteKey)
	if err != nil {
		return nil, fmt.Errorf("error creating exchange handler for store: %v", err)
	}

	usersPublishingRouteKey := "transactions.users"
	usersPublishingHandler, err := a.middlewareHandler.CreateDirectExchangeStandalone(usersPublishingRouteKey)
	if err != nil {
		return nil, fmt.Errorf("error creating exchange handler for users: %v", err)
	}

	return &ExchangeHandlers{
		transactionsPublishing: *transactionsPublishingHandler,
		menuItemsPublishing:    *menuItemsPublishingHandler,
		storePublishing:        *storePublishingHandler,
		usersPublishing:        *usersPublishingHandler,
		resultsQ1Subscription:  *resultsQ1SubscriptionHandler,
		resultsQ2Subscription:  *resultsQ2SubscriptionHandler,
		resultsQ3Subscription:  *resultsQ3SubscriptionHandler,
		resultsQ4Subscription:  *resultsQ4SubscriptionHandler,
	}, nil
}

// Run starts the acceptor loop to accept and handle client connections.
// It runs until shutdown is signaled, accepting one connection at a time.
// Returns an error if accepting fails.
func (a *Acceptor) Run() error {
	a.log.Info("Running and ready to accept connections")
	go a.handleSignal()

	for a.isRunning {
		a.log.Info("Waiting for a new client connection...")

		/// In case the limit is reached, it will block here

		a.limitHandler.Wait()
		conn, err := a.listener.Accept()

		if err != nil {
			a.log.Warningf("Failed 2o accept connection: %v", err)
			return nil
		}

		err = a.CreateNewClient(conn)

		if err != nil {
			a.log.Errorf("Error creating new client: %v", err)
		}

	}

	return nil
}

func (a *Acceptor) CreateNewClient(conn net.Conn) error {

	a.log.Infof("Accepted connection from %s", conn.RemoteAddr().String())
	newId := NewClientUuid()

	exchangeHandlers, err := a.createExchangeHandlers()

	if err != nil {
		return fmt.Errorf("failed to create exchange handlers: %v", err)
	}

	newClient := NewClientHandler(conn, newId, *exchangeHandlers, a.limitHandler)

	a.log.Infof("Assigned client id with short form %s", newId.Short)

	go newClient.Handle()

	a.removeClients()

	a.currClient[newId] = newClient

	return nil
}

func (a *Acceptor) removeClients() {
	for id, client := range a.currClient {
		if !client.IsRunning() {
			a.log.Infof("Removing client %s", id.Short)
			delete(a.currClient, id)
		}
	}
}

// Shutdown gracefully stops the acceptor, closing the listener and current client.
func (a *Acceptor) Shutdown() {
	a.isRunning = false

	if a.listener != nil {
		a.listener.Close()
	}

	for _, client := range a.currClient {
		if client.IsRunning() {
			client.Shutdown()
		}
	}

	a.middlewareHandler.Close()

	a.log.Info("Shutdown complete")
}
