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
	return nil
}

func (ch *ClientHandler) Shutdown() error {
	// Implement the logic to gracefully shutdown the client handler
	return ch.protocol.conn.Close()
}
