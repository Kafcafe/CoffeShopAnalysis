package clientHandler

import (
	logger "common/logger"
	"common/middleware"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ERROR_CHANNEL_BUFFER_SIZE   = 20
	RESULTS_CHANNEL_BUFFER_SIZE = 10

	ACK          = 0
	NACK_REQUEUE = 1
	NACK_DISCARD = 2
)

type ResultsChannels struct {
	resultsQ1Chan chan middleware.Message
	resultsQ2Chan chan middleware.Message
	resultsQ3Chan chan middleware.MessageGrouped
	resultsQ4Chan chan middleware.Message
}

func NewResultsChannels() ResultsChannels {
	return ResultsChannels{
		resultsQ1Chan: make(chan middleware.Message, RESULTS_CHANNEL_BUFFER_SIZE),
		resultsQ2Chan: make(chan middleware.Message, RESULTS_CHANNEL_BUFFER_SIZE),
		resultsQ3Chan: make(chan middleware.MessageGrouped, RESULTS_CHANNEL_BUFFER_SIZE),
		resultsQ4Chan: make(chan middleware.Message, RESULTS_CHANNEL_BUFFER_SIZE),
	}
}

type QueryID int

type ClientHandler struct {
	protocol           *Protocol
	log                *logging.Logger
	ClientId           ClientUuid
	exchangeHandlers   ExchangeHandlers
	errChan            chan middleware.MessageMiddlewareError
	isRunning          bool
	mtx                sync.Mutex
	resultsChans       ResultsChannels
	sentAllResultsChan chan int
}

// NewClientHandler creates a new ClientHandler instance for the given connection.
// Parameters:
//
//	conn: the network connection to handle
//
// Returns a pointer to the ClientHandler.
func NewClientHandler(conn net.Conn, clientId ClientUuid, exchangeHandlers ExchangeHandlers) *ClientHandler {
	protocol := NewProtocol(conn)

	loggerPrefix := fmt.Sprintf("[CL_H-%s]", clientId.Short)

	return &ClientHandler{
		protocol:           protocol,
		log:                logger.GetLoggerWithPrefix(loggerPrefix),
		ClientId:           clientId,
		exchangeHandlers:   exchangeHandlers,
		errChan:            make(chan middleware.MessageMiddlewareError, ERROR_CHANNEL_BUFFER_SIZE),
		isRunning:          true,
		mtx:                sync.Mutex{},
		resultsChans:       NewResultsChannels(),
		sentAllResultsChan: make(chan int, 1),
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

var nums int = 0

func (clh *ClientHandler) processResultsQ1(message amqp.Delivery) error {
	defer clh.answerMessage(NACK_DISCARD, message)

	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		return err
	}

	stringPayload := msg.Payload

	clh.log.Debugf("action: Sending results to client | results: %s | of len: %d", strings.Join(stringPayload, ", "), len(stringPayload))
	clh.log.Debugf("action: Sending results to client | isEOF:", msg.IsEof)

	clh.resultsChans.resultsQ1Chan <- *msg

	if msg.IsEof {
		clh.log.Info("Received EOF message for result")
	} else {
		clh.log.Debugf("Received result message: %v", msg.Payload)
	}

	clh.answerMessage(ACK, message)
	return nil
}

func (clh *ClientHandler) processResultsQ2(message amqp.Delivery) error {
	defer clh.answerMessage(NACK_DISCARD, message)

	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		return err
	}

	stringPayload := msg.Payload

	clh.log.Debugf("action: Sending results to client | results: %s | of len: %d", strings.Join(stringPayload, ", "), len(stringPayload))
	clh.log.Debugf("action: Sending results to client | isEOF:", msg.IsEof)

	clh.resultsChans.resultsQ2Chan <- *msg

	if msg.IsEof {
		clh.log.Info("Received EOF message for result")
	} else {
		clh.log.Debugf("Received result message: %v", msg.Payload)
	}

	clh.answerMessage(ACK, message)
	return nil
}

func (clh *ClientHandler) processResultsQ3(message amqp.Delivery) error {
	defer clh.answerMessage(NACK_DISCARD, message)

	msg, err := middleware.NewMessageGroupedFromBytes(message.Body)
	if err != nil {
		return err
	}

	//stringPayload := msg.Payload
	//clh.log.Debugf("action: Sending results to client | results: %s | of len: %d", strings.Join(stringPayload, ", "), len(stringPayload))
	clh.log.Debugf("action: Sending results to client | isEOF:", msg.IsEof)

	clh.resultsChans.resultsQ3Chan <- *msg

	if msg.IsEof {
		clh.log.Info("Received EOF message for result")
	} else {
		clh.log.Debugf("Received result message: %v", msg.Payload)
	}

	clh.answerMessage(ACK, message)
	return nil
}

func (clh *ClientHandler) processResultsQ4(message amqp.Delivery) error {
	defer clh.answerMessage(NACK_DISCARD, message)

	msg, err := middleware.NewMessageFromBytes(message.Body)
	if err != nil {
		return err
	}

	//stringPayload := msg.Payload
	//clh.log.Debugf("action: Sending results to client | results: %s | of len: %d", strings.Join(stringPayload, ", "), len(stringPayload))
	clh.log.Debugf("action: Sending results to client | isEOF:", msg.IsEof)

	clh.resultsChans.resultsQ4Chan <- *msg

	if msg.IsEof {
		clh.log.Info("Received EOF message for result")
	} else {
		clh.log.Debugf("Received result message: %v", msg.Payload)
	}

	clh.answerMessage(ACK, message)
	return nil
}

func (clh *ClientHandler) launchCentralResultDispatching() {
	// Track EOF for each query
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
		case msg := <-clh.resultsChans.resultsQ1Chan:
			if msg.IsEof {
				eofFlags[1] = true
			}
			if err := clh.protocol.SendResults(1, msg.Payload, msg.IsEof); err != nil {
				clh.log.Errorf("Error sending result to client for query 1: %v", err)
			}

		case msg := <-clh.resultsChans.resultsQ2Chan:
			if msg.IsEof {
				eofFlags[2] = true
			}
			if err := clh.protocol.SendResults(2, msg.Payload, msg.IsEof); err != nil {
				clh.log.Errorf("Error sending result to client for query 2: %v", err)
			}

		case msg := <-clh.resultsChans.resultsQ3Chan:
			if msg.IsEof {
				eofFlags[3] = true
			}
			parsedPayload, err := json.Marshal(msg.Payload)
			if err != nil {
				clh.log.Errorf("Failed marshalling payload for query 3: %v", err)
				continue
			}
			jsonString := string(parsedPayload)
			if err := clh.protocol.SendResults(3, []string{jsonString}, msg.IsEof); err != nil {
				clh.log.Errorf("Error sending result to client for query 3: %v", err)
			}

		case msg := <-clh.resultsChans.resultsQ4Chan:
			if msg.IsEof {
				eofFlags[4] = true
			}
			if err := clh.protocol.SendResults(4, msg.Payload, msg.IsEof); err != nil {
				clh.log.Errorf("Error sending result to client for query 4: %v", err)
			}
		}
	}
}

func (clh *ClientHandler) launchResultsProcessing() {
	go clh.launchCentralResultDispatching()
	clh.exchangeHandlers.resultsQ1Subscription.StartConsuming(clh.processResultsQ1, clh.errChan)
	clh.exchangeHandlers.resultsQ2Subscription.StartConsuming(clh.processResultsQ2, clh.errChan)
	clh.exchangeHandlers.resultsQ3Subscription.StartConsuming(clh.processResultsQ3, clh.errChan)
	clh.exchangeHandlers.resultsQ4Subscription.StartConsuming(clh.processResultsQ4, clh.errChan)

	for err := range clh.errChan {
		if err != middleware.MessageMiddlewareSuccess {
			clh.log.Errorf("Error found while filtering message of type: %v", err)
		}

		if !clh.isRunning {
			clh.log.Info("Inside error loop: breaking")
			break
		}
	}

	clh.exchangeHandlers.resultsQ1Subscription.Close()
	clh.exchangeHandlers.resultsQ2Subscription.Close()
	clh.exchangeHandlers.resultsQ3Subscription.Close()
	clh.exchangeHandlers.resultsQ4Subscription.Close()
}

// Handle processes the client connection by receiving and handling data types and files.
// Returns an error if any step fails.
func (clh *ClientHandler) Handle() error {
	clh.log.Info("Handling client connection")

	// Receive the number of data types to process
	amountOfdataTypes, err := clh.protocol.rcvAmountOfDataTypes()
	if err != nil {
		return fmt.Errorf("error receiving amount of dataTypes: %v", err)
	}

	go clh.launchResultsProcessing()

	// Loop over each data type
	for range amountOfdataTypes {
		clh.log.Infof("Number of dataTypes to receive: %v", amountOfdataTypes)

		dataType, amountOfFiles, err := clh.handleDataType()
		if err != nil {
			clh.log.Errorf("Error handling dataType: %v", err)
		}

		clh.log.Infof("Number of files to receive for dataType %s: %d", dataType, amountOfFiles)
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
		return "", 0, fmt.Errorf("error receiving files dataType: %v", err)
	}

	clh.log.Infof("Received files dataType: %s", dataType)

	amountOfFiles, err = clh.protocol.RcvAmountOfFiles()
	clh.log.Infof("Amount of files to receive for dataType %s: %d", dataType, amountOfFiles)

	if err != nil {
		return "", 0, fmt.Errorf("error receiving amount of files for dataType %s: %v", dataType, err)
	}

	err = clh.processDataType(amountOfFiles, dataType)
	if err != nil {
		return "", 0, fmt.Errorf("error processing files for dataType %s: %v", dataType, err)
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
		clh.log.Infof("nums: %d", nums)
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

	msg := middleware.NewMessage(dataType, clh.ClientId.Full, cleanBatch, isEof)
	msgBytes, err := msg.ToBytes()
	if err != nil {
		return err
	}

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
		batch, isLast, err := clh.protocol.ReceiveBatch()

		if err != nil {
			return fmt.Errorf("error receiving file batch for dataType %s: %v", dataType, err)
		}

		// If this is the last batch, stop receiving
		if isLast {
			receivingFile = false
			break
		}

		batchCounter++
		clh.log.Debugf("Received batch %d for dataType %s", batchCounter, dataType)

		isEof := false
		err = clh.dispatchBatchToMiddleware(dataType, batch, isEof)
		if err != nil {
			return fmt.Errorf("error dispatching batch to middleware: %v", err)
		}
	}
	return nil
}

func (clh *ClientHandler) SendResult() error {
	clh.log.Info("Sending result to client - Not implemented")

	return nil
}

// Shutdown closes the protocol connection.
// Returns an error if closing fails.
func (clh *ClientHandler) Shutdown() error {
	clh.isRunning = false

	if clh.protocol != nil {
		clh.protocol.Shutdown()
	}

	clh.exchangeHandlers.transactionsPublishing.Close()
	clh.exchangeHandlers.resultsQ1Subscription.Close()
	return nil
}
