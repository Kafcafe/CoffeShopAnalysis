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
	// Implement the logic to handle client requests using the protocol
	log.Info("Handling client connection")
	// Example: receive a message from the client

	amountOfTopics, err := ch.protocol.rcvAmountOfTopics()

	if err != nil {
		log.Errorf("Error receiving amount of topics: %v", err)
		return err
	}

	log.Info("Number of topics to receive: %d", amountOfTopics)

	for i := 0; i < amountOfTopics; i++ {

		topic, err := ch.protocol.ReceiveFilesTopic()
		if err != nil {
			log.Errorf("Error receiving files topic: %v", err)
			return err
		}

		log.Infof("Received files topic: %s", topic)

		err = ch.FilesPerTopic(topic)

		if err != nil {
			log.Errorf("Error handling files for topic %s: %v", topic, err)
			return err
		}
	}

	return nil

}

func (ch *ClientHandler) FilesPerTopic(topic string) error {
	amountOfFiles, err := ch.protocol.rcvAmountOfFiles()

	if err != nil {
		log.Errorf("Error receiving amount of files for topic %s: %v", topic, err)
		return err
	}

	for i := 0; i < amountOfFiles; i++ {
	}

	return nil
}

func (ch *ClientHandler) Shutdown() error {
	// Implement the logic to gracefully shutdown the client handler
	return ch.protocol.conn.Close()
}
