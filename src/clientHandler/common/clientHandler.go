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

	amountOfTopics, err := ch.protocol.rcvAmountOfTopics()

	if err != nil {
		log.Errorf("Error receiving amount of topics: %v", err)
		return err
	}

	log.Info("Number of topics to receive: %d", amountOfTopics)

	topic, amountOfFiles, err := ch.handleTopic()

	if err != nil {
		log.Errorf("Error handling topic: %v", err)
	}

	log.Info("Number of files to receive for topic %s: %d", topic, amountOfFiles)

	return nil
}

func (ch *ClientHandler) handleTopic() (string, int, error) {
	topic, err := ch.protocol.ReceiveFilesTopic()

	if err != nil {
		log.Errorf("Error receiving files topic: %v", err)
		return "", 0, err
	}

	log.Infof("Received files topic: %s", topic)

	amountOfFiles, err := ch.protocol.rcvAmountOfFiles()

	if err != nil {
		log.Errorf("Error receiving amount of files for topic %s: %v", topic, err)
		return "", 0, err
	}

	err = ch.processTopics(amountOfFiles, topic)

	if err != nil {
		log.Errorf("Error processing files for topic %s: %v", topic, err)
		return "", 0, err
	}

	return topic, amountOfFiles, nil
}

func (ch *ClientHandler) processTopics(amountOfFiles int, topic string) error {
	currFile := 0
	for currFile < amountOfFiles {
		err := ch.processFile(topic)
		if err != nil {
			log.Errorf("Error processing file %d for topic %s: %v", currFile, topic, err)
			return err
		}
		currFile++
	}
	return nil
}

func (ch *ClientHandler) processFile(topic string) error {
	receivingFile := true
	batchCounter := 0
	for receivingFile {
		batch, isLast, err := ch.protocol.ReceiveBatch()

		if err != nil {
			log.Errorf("Error receiving file batch for topic %s: %v", topic, err)
			return err
		}

		batchCounter++
		log.Infof("Received batch %d for topic %s", batchCounter, topic)
		log.Infof("Batch data: %s", batch)

		err = ch.protocol.ConfirmBatchReceived()
		if err != nil {
			log.Errorf("Error confirming batch %d for topic %s: %v", batchCounter, topic, err)
			return err
		}

		if isLast {
			return nil
		}

	}
	return nil
}

func (ch *ClientHandler) Shutdown() error {
	// Implement the logic to gracefully shutdown the client handler
	return ch.protocol.conn.Close()
}
