package client

import (
	"fmt"
	"net"
)

type Protocol struct {
	serverAddress string
	conn          net.Conn
}

func NewProtocol(serverAddress string) (*Protocol, error) {

	conn, err := net.Dial("tcp", serverAddress)

	if err != nil {
		return nil, fmt.Errorf("failed to connect to server at %s", serverAddress)
	}

	return &Protocol{
		serverAddress: serverAddress,
		conn:          conn,
	}, nil
}

func (p *Protocol) SendBatch(batch *Batch) error {
	// Implement batch sending logic here
	return nil
}

func (p *Protocol) receivedConfirmation() error {
	// Implement confirmation receiving logic here
	return nil
}

func (p *Protocol) FinishSendingFilesOf(pattern string) error {
	// Implement finish sending files logic here
	return nil
}
