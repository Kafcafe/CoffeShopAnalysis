package clientHandler

import (
	"ClientHandler/logger"
	"fmt"
	"net"

	"github.com/op/go-logging"
)

type ClientHandler struct {
	protocol *Protocol
	log      *logging.Logger
}

// NewClientHandler creates a new ClientHandler instance for the given connection.
// Parameters:
//
//	conn: the network connection to handle
//
// Returns a pointer to the ClientHandler.
func NewClientHandler(conn net.Conn) *ClientHandler {
	protocol := NewProtocol(conn)

	return &ClientHandler{
		protocol: protocol,
		log:      logger.GetLoggerWithPrefix("[CLIENT_HANDLER]"),
	}
}

// Handle processes the client connection by receiving and handling data types and files.
// Returns an error if any step fails.
func (cl_h *ClientHandler) Handle() error {
	cl_h.log.Info("Handling client connection")

	// Receive the number of data types to process
	amountOfdataTypes, err := cl_h.protocol.rcvAmountOfDataTypes()
	if err != nil {
		return fmt.Errorf("Error receiving amount of dataTypes: %v", err)
	}

	// Loop over each data type
	for range amountOfdataTypes {
		cl_h.log.Infof("Number of dataTypes to receive: %v", amountOfdataTypes)

		dataType, amountOfFiles, err := cl_h.handleDataType()
		if err != nil {
			cl_h.log.Errorf("Error handling dataType: %v", err)
		}

		cl_h.log.Infof("Number of files to receive for dataType %s: %d", dataType, amountOfFiles)
	}

	return nil
}

// handleDataType receives and processes a single data type, including its files.
// Returns the data type name, number of files, and any error.
func (cl_h *ClientHandler) handleDataType() (dataType string, amountOfFiles int, err error) {
	dataType, err = cl_h.protocol.ReceiveFilesDataType()

	if err != nil {
		return "", 0, fmt.Errorf("Error receiving files dataType: %v", err)
	}

	cl_h.log.Infof("Received files dataType: %s", dataType)

	amountOfFiles, err = cl_h.protocol.RcvAmountOfFiles()
	cl_h.log.Infof("Amount of files to receive for dataType %s: %d", dataType, amountOfFiles)

	if err != nil {
		return "", 0, fmt.Errorf("Error receiving amount of files for dataType %s: %v", dataType, err)
	}

	err = cl_h.processdataType(amountOfFiles, dataType)
	if err != nil {
		return "", 0, fmt.Errorf("Error processing files for dataType %s: %v", dataType, err)
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
func (cl_h *ClientHandler) processdataType(amountOfFiles int, dataType string) error {
	for currFile := 0; currFile < amountOfFiles; currFile++ {
		cl_h.log.Infof("Processing file %d for dataType %s", currFile, dataType)

		err := cl_h.processFile(dataType)
		if err != nil {
			return fmt.Errorf("Error processing file %d for dataType %s: %v", currFile, dataType, err)
		}

		cl_h.log.Infof("Finished processing file %d for dataType %s", currFile, dataType)
	}

	cl_h.log.Infof("Finished processing all %d files for dataType %s", amountOfFiles, dataType)
	return nil
}

// processFile processes a single file by receiving its batches until completion.
// Parameters:
//
//	dataType: the type of data for the file
//
// Returns an error if processing fails.
func (cl_h *ClientHandler) processFile(dataType string) error {
	// Flag to control the receiving loop
	receivingFile := true
	batchCounter := 0

	// Loop to receive batches until the file is complete
	for receivingFile {
		cl_h.log.Infof("Receiving batch %d for dataType %s", batchCounter, dataType)
		batch, isLast, err := cl_h.protocol.ReceiveBatch()

		if err != nil {
			return fmt.Errorf("Error receiving file batch for dataType %s: %v", dataType, err)
		}

		// If this is the last batch, stop receiving
		if isLast {
			receivingFile = false
			break
		}

		batchCounter++
		cl_h.log.Infof("Received batch %d for dataType %s", batchCounter, dataType)

		// stays for compiling porposes, batch is not process
		// Placeholder for batch processing (not implemented)
		cl_h.log.Infof("Batch data: %s", batch)

		err = cl_h.protocol.ConfirmBatchReceived()
		if err != nil {
			return fmt.Errorf("Error confirming batch %d for dataType %s: %v", batchCounter, dataType, err)
		}
	}
	return nil
}

// Shutdown closes the protocol connection.
// Returns an error if closing fails.
func (cl_h *ClientHandler) Shutdown() error {
	if cl_h.protocol != nil {
		cl_h.protocol.Shutdown()
	}

	return nil
}
