package clientHandler

import (
	"net"
)

type ClientHandler struct {
	protocol *Protocol
}

func NewClientHandler(conn net.Conn) *ClientHandler {
	protocol := NewProtocol(conn)
	return &ClientHandler{
		protocol: protocol,
	}
}

func (ch *ClientHandler) Handle() error {
	log.Info("Handling client connection")

	amountOfdataTypes, err := ch.protocol.rcvAmountOfDataTypes()

	if err != nil {
		log.Errorf("Error receiving amount of dataTypes: %v", err)
		return err
	}

	for i := 0; i < amountOfdataTypes; i++ {

		log.Info("Number of dataTypes to receive: %v", amountOfdataTypes)

		dataType, amountOfFiles, err := ch.handledataType()

		if err != nil {
			log.Errorf("Error handling dataType: %v", err)
		}

		log.Info("Number of files to receive for dataType %s: %d", dataType, amountOfFiles)
	}

	return nil
}

func (ch *ClientHandler) handledataType() (string, int, error) {
	dataType, err := ch.protocol.ReceiveFilesdataType()

	if err != nil {
		log.Errorf("Error receiving files dataType: %v", err)
		return "", 0, err
	}

	log.Infof("Received files dataType: %s", dataType)

	amountOfFiles, err := ch.protocol.rcvAmountOfFiles()

	log.Info("Amount of files to receive for dataType %s: %d", dataType, amountOfFiles)

	if err != nil {
		log.Errorf("Error receiving amount of files for dataType %s: %v", dataType, err)
		return "", 0, err
	}

	err = ch.processdataType(amountOfFiles, dataType)

	if err != nil {
		log.Errorf("Error processing files for dataType %s: %v", dataType, err)
		return "", 0, err
	}

	return dataType, amountOfFiles, nil
}

func (ch *ClientHandler) processdataType(amountOfFiles int, dataType string) error {
	currFile := 0
	for currFile < amountOfFiles {
		log.Infof("Processing file %d for dataType %s", currFile, dataType)
		err := ch.processFile(dataType)
		if err != nil {
			log.Errorf("Error processing file %d for dataType %s: %v", currFile, dataType, err)
			return err
		}
		log.Infof("Finished processing file %d for dataType %s", currFile, dataType)
		currFile++
	}
	log.Infof("Finished processing all %d files for dataType %s", amountOfFiles, dataType)
	return nil
}

func (ch *ClientHandler) processFile(dataType string) error {
	receivingFile := true
	batchCounter := 0
	for receivingFile {
		log.Infof("Receiving batch %d for dataType %s", batchCounter, dataType)
		batch, isLast, err := ch.protocol.ReceiveBatch()

		if err != nil {
			log.Errorf("Error receiving file batch for dataType %s: %v", dataType, err)
			return err
		}

		batchCounter++
		log.Infof("Received batch %d for dataType %s", batchCounter, dataType)

		// stays for compiling porposes, batch is not process
		log.Infof("Batch data: %s", batch)

		err = ch.protocol.ConfirmBatchReceived()
		if err != nil {
			log.Errorf("Error confirming batch %d for dataType %s: %v", batchCounter, dataType, err)
			return err
		}

		if isLast {
			return nil
		}

	}
	return nil
}

func (ch *ClientHandler) Shutdown() error {
	return ch.protocol.conn.Close()
}
