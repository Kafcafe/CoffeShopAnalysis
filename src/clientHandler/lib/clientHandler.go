package clientHandler

import (
	logger "common/logger"
	"common/middleware"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ERROR_CHANNEL_BUFFER_SIZE   = 20
	RESULTS_CHANNEL_BUFFER_SIZE = 10

	ACK          = 0
	NACK_REQUEUE = 1
	NACK_DISCARD = 2

	TIME_TO_PROCESS_ALREADY_BUFFERED_RESULTS = 15
)

type QueryID int
type DataType = string

type ClientHandler struct {
	protocol                                *Protocol
	log                                     *logging.Logger
	ClientId                                ClientUuid
	exchangeHandlers                        ExchangeHandlers
	errChan                                 chan middleware.MessageMiddlewareError
	isRunning                               bool
	mtx                                     sync.Mutex
	resultsChan                             chan middleware.Message
	sentAllResultsChan                      chan int
	timeToProcessAlreadyBufferedResultsChan chan int
	connectionBroken                        chan int
	emittedCount                            map[DataType]int
	limitHandler                            *ConnectionLimit
	middlewareHandler                       *middleware.MiddlewareHandler
	sessionManager                          *SessionManager
}

// NewClientHandler creates a new ClientHandler instance for the given connection.
// Parameters:
//
//	conn: the network connection to handle
//
// Returns a pointer to the ClientHandler.
func NewClientHandler(conn net.Conn, clientId ClientUuid, exchangeHandlers ExchangeHandlers, limitRef *ConnectionLimit, middlewareHandler *middleware.MiddlewareHandler, sessionManager *SessionManager) *ClientHandler {
	protocol := NewProtocol(conn)

	loggerPrefix := fmt.Sprintf("[CL_H-%s]", clientId.Short)

	return &ClientHandler{
		protocol:                                protocol,
		log:                                     logger.GetLoggerWithPrefix(loggerPrefix),
		ClientId:                                clientId,
		exchangeHandlers:                        exchangeHandlers,
		errChan:                                 make(chan middleware.MessageMiddlewareError, ERROR_CHANNEL_BUFFER_SIZE),
		isRunning:                               true,
		mtx:                                     sync.Mutex{},
		resultsChan:                             make(chan middleware.Message, RESULTS_CHANNEL_BUFFER_SIZE),
		sentAllResultsChan:                      make(chan int, 1),
		emittedCount:                            make(map[DataType]int),
		limitHandler:                            limitRef,
		middlewareHandler:                       middlewareHandler,
		timeToProcessAlreadyBufferedResultsChan: make(chan int, 1),
		connectionBroken:                        make(chan int, 1),
		sessionManager:                          sessionManager,
	}
}

func (clh *ClientHandler) answerMessage(ackType int, message amqp.Delivery) {
	switch ackType {
	case ACK:
	case NACK_REQUEUE:
		message.Nack(false, true)
	case NACK_DISCARD:
		message.Nack(false, false)
	}
}

func (clh *ClientHandler) processResults(message amqp.Delivery) error {
	defer clh.answerMessage(NACK_DISCARD, message)

	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		return err
	}

	clh.log.Debugf("action: Sending results to client | isEOF: %t", msg.IsEof)

	clh.resultsChan <- *msg

	if msg.IsEof {
		clh.log.Info("Received EOF message for result")
	} else {
		clh.log.Debugf("Received result message: %v", msg.Payload)
	}

	clh.answerMessage(ACK, message)
	return nil
}

func (clh *ClientHandler) dispatchResultMessage(msg *middleware.Message, eofFlags *map[int]bool) error {
	queryId := msg.QueryId

	if queryId <= 0 || queryId > 4 {
		clh.log.Warningf("Unrecognized queryId")
		return nil
	}

	if msg.IsEof {
		(*eofFlags)[queryId] = true
	}

	var cleanResult []string = msg.Payload
	var err error = nil

	switch msg.QueryId {
	case 1:
		cleanResult, err = cleanTransactionResults(msg.Payload)
	case 2:
		cleanResult, err = cleanTransactionItemsResults(msg.Payload)
	}

	if err != nil {
		clh.log.Errorf("Error cleaning result for query %d, sending results raw: %v", msg.QueryId, err)
		clh.mtx.Lock()
		err := clh.protocol.SendResults(uint32(queryId), msg.Payload, msg.IsEof)
		clh.mtx.Unlock()
		if err != nil {
			clh.log.Errorf("Error sending result to client for query %d: %v", queryId, err)
		}
		return err
	}

	clh.mtx.Lock()
	err = clh.protocol.SendResults(uint32(queryId), cleanResult, msg.IsEof)
	clh.mtx.Unlock()

	if err != nil {
		clh.log.Errorf("Error sending result to client for query %d: %v", queryId, err)
	}

	return nil
}

func (clh *ClientHandler) launchCentralResultDispatching() {
	// Track EOF for each query
	var dispathMessage bool = true
	eofFlags := map[int]bool{
		1: false,
		2: false,
		3: false,
		4: false,
	}

	for {
		// If all channels flagged EOF -> break out
		if eofFlags[1] && eofFlags[2] && eofFlags[3] && eofFlags[4] {
			clh.log.Infof("All queries EOF received, shutting down dispatcher")
			clh.sentAllResultsChan <- 0
			return
		}

		select {
		case msg := <-clh.resultsChan:
			if !dispathMessage {
				continue
			}

			err := clh.dispatchResultMessage(&msg, &eofFlags)
			if err != nil {
				return
			}
		case <-clh.timeToProcessAlreadyBufferedResultsChan:
			clh.log.Infof("Finished processing buffered results")
			return
		case <-clh.connectionBroken:
			clh.log.Infof("Finished processing buffered results")
			dispathMessage = false
		}
	}
}

func (clh *ClientHandler) launchResultsProcessing() {
	go clh.launchCentralResultDispatching()
	clh.exchangeHandlers.resultsSubscription.StartConsuming(clh.processResults, clh.errChan)

	for err := range clh.errChan {
		if err != middleware.MessageMiddlewareSuccess {
			clh.log.Errorf("Error found while filtering message of type: %v", err)
		}

		if !clh.isRunning {
			clh.log.Info("Inside error loop: breaking")
			break
		}
	}

	clh.exchangeHandlers.resultsSubscription.Close()
}

// Handle processes the client connection by receiving and handling data types and files.
// Returns an error if any step fails.
func (clh *ClientHandler) Handle() error {
	clh.log.Info("Handling client connection")
	defer clh.Shutdown()

	if err := clh.handleHandshake(); err != nil {
		return err
	}

	dataType, err := clh.processDataTypes()
	if err != nil {
		return clh.handleFailure(err, dataType)
	}

	<-clh.sentAllResultsChan
	clh.log.Infof("Finished sending results to client")

	return nil
}

// handleDataType receives and processes a single data type, including its files.
// Returns the data type name, number of files, and any error.
func (clh *ClientHandler) handleDataType() (dataType string, amountOfFiles int, err error) {
	dataType, err = clh.protocol.ReceiveFilesDataType()
	if err != nil {
		return dataType, 0, fmt.Errorf("error receiving files dataType: %v", err)
	}

	clh.log.Infof("Received files dataType: %s", dataType)

	clh.mtx.Lock()
	err = clh.protocol.SendAck()
	clh.mtx.Unlock()
	if err != nil {
		return dataType, 0, fmt.Errorf("error sending ACK: %v", err)
	}

	amountOfFiles, err = clh.protocol.RcvAmountOfFiles()
	clh.log.Infof("Amount of files to receive for dataType %s: %d", dataType, amountOfFiles)

	if err != nil {
		return dataType, 0, fmt.Errorf("error receiving amount of files for dataType %s: %v", dataType, err)
	}

	clh.mtx.Lock()
	err = clh.protocol.SendAck()
	clh.mtx.Unlock()
	if err != nil {
		return dataType, 0, fmt.Errorf("error sending ACK: %v", err)
	}

	err = clh.processDataType(amountOfFiles, dataType)
	if err != nil {
		return dataType, 0, fmt.Errorf("error processing files for dataType %s: %v", dataType, err)
	}

	return dataType, amountOfFiles, nil
}

// processdataType processes all files for a given data type.
// Parameters:
//
//	amountOfFiles: number of files to process
//	dataType: the type of data
//
// Returns an error if processing fails.
func (clh *ClientHandler) processDataType(amountOfFiles int, dataType string) error {
	for currFile := 0; currFile < amountOfFiles; currFile++ {
		clh.log.Infof("Processing file %d for dataType %s", currFile, dataType)

		err := clh.processFile(dataType)
		if err != nil {
			return fmt.Errorf("error processing file %d for dataType %s: %v", currFile, dataType, err)
		}

		clh.log.Infof("Finished processing file %d for dataType %s", currFile, dataType)
	}

	isEof := true
	err := clh.dispatchBatchToMiddleware(dataType, []string{}, isEof)
	if err != nil {
		return fmt.Errorf("error dispatching EOF to middleware: %v", err)
	}

	clh.log.Infof("Finished processing all %d files for dataType %s", amountOfFiles, dataType)
	return nil
}

func (clh *ClientHandler) cleanBatch(dataType string, batch []string) (cleanBatch []string, err error) {
	switch dataType {
	case "transactions":
		return cleanTransactions(batch)
	case "transaction_items":
		return cleanTransactionItems(batch)
	case "menu":
		return cleanMenuItems(batch)
	case "store":
		return cleanStores(batch)
	case "users":
		return cleanUsers(batch)
	default:
		clh.log.Infof("Batch clean for %s dataType not available", dataType)
		return batch, nil
	}
}

func (clh *ClientHandler) dispatchBatchToMiddleware(dataType string, batch []string, isEof bool) error {
	cleanBatch, err := clh.cleanBatch(dataType, batch)
	if err != nil {
		return err
	}

	if _, exists := clh.emittedCount[dataType]; !exists {
		clh.emittedCount[dataType] = 0
	}

	msg := middleware.NewMessageWithPayload(dataType, clh.ClientId.Full, cleanBatch, isEof, middleware.QUERY_ID_NOT_SET)
	if isEof {
		clh.log.Infof("Dispatching EOF for dataType %s with total emitted %d", dataType, clh.emittedCount[dataType])
		msg.TotalEmitted = clh.emittedCount[dataType]
	}
	msgBytes, err := msg.ToBytes()
	if err != nil {
		return err
	}

	clh.emittedCount[dataType] += 1
	res := middleware.MessageMiddlewareSuccess
	err = nil

	switch dataType {
	case "transactions":
		res = clh.exchangeHandlers.transactionsPublishing.Send(msgBytes)
		err = fmt.Errorf("problem while sending batch of dataType %s", dataType)
	case "transaction_items":
		res = clh.exchangeHandlers.transactionsPublishing.Send(msgBytes)
		err = fmt.Errorf("problem while sending batch of dataType %s", dataType)
	case "menu":
		res = clh.exchangeHandlers.menuItemsPublishing.Send(msgBytes)
		err = fmt.Errorf("problem while sending batch of dataType %s", dataType)
	case "store":
		res = clh.exchangeHandlers.storePublishing.Send(msgBytes)
		err = fmt.Errorf("problem while sending batch of dataType %s", dataType)
	case "users":
		res = clh.exchangeHandlers.usersPublishing.Send(msgBytes)
		err = fmt.Errorf("problem while sending batch of dataType %s", dataType)
	default:
		clh.log.Infof("Dispatch for %s dataType not available", dataType)
	}

	if res != middleware.MessageMiddlewareSuccess {
		return err
	}

	clh.log.Debugf("Successfully sent batch of %s", dataType)

	return nil
}

// processFile processes a single file by receiving its batches until completion.
// Parameters:
//
//	dataType: the type of data for the file
//
// Returns an error if processing fails.
func (clh *ClientHandler) processFile(dataType string) error {
	// Flag to control the receiving loop
	receivingFile := true
	batchCounter := 0

	// Loop to receive batches until the file is complete
	for receivingFile {
		clh.log.Debugf("Receiving batch %d for dataType %s", batchCounter, dataType)

		batch, isLastBatch, err := clh.protocol.ReceiveBatch()
		if err != nil {
			return fmt.Errorf("error receiving file batch for dataType %s: %v", dataType, err)
		}

		// If this is the last batch, stop receiving
		if isLastBatch {
			receivingFile = false
			clh.mtx.Lock()
			err = clh.protocol.SendAck()
			clh.mtx.Unlock()
			if err != nil {
				return fmt.Errorf("error sending ACK: %v", err)
			}
			break
		}

		batchCounter++
		clh.log.Debugf("Received batch %d for dataType %s", batchCounter, dataType)

		isEof := false
		err = clh.dispatchBatchToMiddleware(dataType, batch, isEof)
		if err != nil {
			return fmt.Errorf("error dispatching batch to middleware: %v", err)
		}

		clh.mtx.Lock()
		err = clh.protocol.SendAck()
		clh.mtx.Unlock()
		if err != nil {
			return fmt.Errorf("error sending ACK: %v", err)
		}
	}
	return nil
}

func (clh *ClientHandler) SendResult() error {
	clh.log.Info("Sending result to client - Not implemented")

	return nil
}

func (clh *ClientHandler) IsRunning() bool {
	return clh.isRunning
}

// Shutdown closes the protocol connection.
// Returns an error if closing fails.
func (clh *ClientHandler) Shutdown() error {
	if !clh.isRunning {
		return nil
	}

	clh.isRunning = false

	if clh.protocol != nil {
		clh.protocol.Shutdown()
	}

	clh.exchangeHandlers.transactionsPublishing.Close()
	clh.exchangeHandlers.resultsSubscription.Close()
	clh.middlewareHandler.CloseChannel()
	clh.log.Info("About to notify limit handler of disconnection")
	clh.limitHandler.Signal()
	clh.sessionManager.UnregisterSession(clh.ClientId.Full)
	return nil
}

func (clh *ClientHandler) handleHandshake() error {
	for {
		opCode, err := clh.protocol.ReceiveOpCode()
		if err != nil {
			return fmt.Errorf("error receiving operation code: %v", err)
		}

		switch opCode {
		case ConnectionRequest:
			return clh.handleConnectionRequest()

		case ReconnectionRequest:
			reconnected, err := clh.handleReconnectionRequest()
			if err != nil {
				return err
			}
			if reconnected {
				return nil
			}
			// If not reconnected, loop to receive next OpCode

		default:
			return fmt.Errorf("unexpected operation code: %d", opCode)
		}
	}
}

func (clh *ClientHandler) handleConnectionRequest() error {
	clh.log.Info("ConnectionRequest received")

	if !clh.limitHandler.TryAcquire() {
		clh.log.Info("Connection limit reached, sending WAIT")
		clh.mtx.Lock()
		err := clh.protocol.SendWait()
		clh.mtx.Unlock()
		if err != nil {
			return fmt.Errorf("error sending WAIT: %v", err)
		}
		clh.limitHandler.Wait()
	}

	clh.mtx.Lock()
	err := clh.protocol.SendBeginWithClientId(clh.ClientId.Full)
	clh.mtx.Unlock()
	if err != nil {
		return fmt.Errorf("error sending Begin and Session ID: %v", err)
	}
	clh.log.Infof("Sent Begin for ClientID: %s", clh.ClientId.Full)

	clh.sessionManager.RegisterSession(clh.ClientId.Full)
	return nil
}

func (clh *ClientHandler) handleReconnectionRequest() (reconnected bool, err error) {
	clientId, err := clh.protocol.RcvClientId()
	if err != nil {
		return false, fmt.Errorf("error receiving client ID for reconnection: %v", err)
	}

	if clh.sessionManager.ValidateSession(clientId) {
		clh.log.Infof("Reconnection accepted for client %s", clientId)
		clh.mtx.Lock()
		err := clh.protocol.SendReconnectionAccept()
		clh.mtx.Unlock()
		if err != nil {
			return false, fmt.Errorf("error sending ReconnectionAccept: %v", err)
		}
		clh.sessionManager.RegisterSession(clientId)
		return true, nil
	} else {
		clh.log.Infof("Reconnection denied for client %s", clientId)
		clh.mtx.Lock()
		err := clh.protocol.SendReconnectionDenied()
		clh.mtx.Unlock()
		if err != nil {
			return false, fmt.Errorf("error sending ReconnectionDenied: %v", err)
		}

		// Client should start a new connection, so false is returned to indicate no reconnection
		return false, nil
	}
}

func (clh *ClientHandler) processDataTypes() (string, error) {
	amountOfdataTypes, err := clh.protocol.rcvAmountOfDataTypes()
	if err != nil {
		return "", fmt.Errorf("error receiving amount of dataTypes: %v", err)
	}
	clh.log.Infof("Amount of dataTypes to receive: %d", amountOfdataTypes)

	clh.mtx.Lock()
	err = clh.protocol.SendAck()
	clh.mtx.Unlock()
	if err != nil {
		return "", fmt.Errorf("error sending ACK: %v", err)
	}

	go clh.launchResultsProcessing()

	// Loop over each data type
	var dataType string
	var amountOfFiles int

	for range amountOfdataTypes {
		clh.log.Infof("Number of dataTypes to receive: %v", amountOfdataTypes)

		dataType, amountOfFiles, err = clh.handleDataType()
		if err != nil {
			clh.log.Errorf("Error handling dataType: %v", err)
			return dataType, err
		}

		clh.log.Infof("Number of files to receive for dataType %s: %d", dataType, amountOfFiles)
	}
	return dataType, nil
}

func (clh *ClientHandler) handleFailure(err error, dataType string) error {
	clh.log.Warningf("Sending EOF after failure for error: %v", err)
	errSending := clh.dispatchBatchToMiddleware(dataType, []string{}, true)

	var errToReturn error = nil
	if errSending != nil {
		errToReturn = fmt.Errorf("error dispatching EOF to middleware: %v", errSending)
	}

	clh.log.Infof("Processing buffered results for %d seconds", TIME_TO_PROCESS_ALREADY_BUFFERED_RESULTS)
	clh.connectionBroken <- 0
	<-time.After(time.Duration(TIME_TO_PROCESS_ALREADY_BUFFERED_RESULTS) * time.Second)
	clh.log.Infof("Finished buffered time for results (was %ds)", TIME_TO_PROCESS_ALREADY_BUFFERED_RESULTS)
	clh.timeToProcessAlreadyBufferedResultsChan <- 0

	if errToReturn != nil {
		return errToReturn
	}

	errToReturn = fmt.Errorf("error while processing: %v", err)
	return errToReturn
}
