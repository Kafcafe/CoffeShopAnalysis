package client

import (
	"encoding/binary"
	"fmt"
	"net"
)

const (
	BatchRcvCode = 0x01
	EndOfBatch   = 0x02
	MoreBatches  = 0x03
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

func (p *Protocol) sendAmountOfTopics(amount int) error {
	lenBytes := p.htonsUint32(uint32(amount))

	if err := p.sendAll(lenBytes); err != nil {
		return err
	}

	return nil
}

func (p *Protocol) SendFilesTopic(pattern string, amount int) error {
	dataLen := uint32(len(pattern))
	lenBytes := p.htonsUint32(dataLen)

	if err := p.sendAll(lenBytes); err != nil {
		return err
	}

	if err := p.sendAll([]byte(pattern)); err != nil {
		return err
	}

	return nil
}

func (p *Protocol) SendBatch(batch *Batch) error {

	opCode := []byte{MoreBatches}

	if err := p.sendAll(opCode); err != nil {
		return err
	}

	dataLen := uint32(len(batch.Items))
	lenBytes := p.htonsUint32(dataLen)

	if err := p.sendAll(lenBytes); err != nil {
		return err
	}

	for _, item := range batch.Items {
		itemLen := uint32(len(item))
		itemLenBytes := p.htonsUint32(itemLen)

		if err := p.sendAll(itemLenBytes); err != nil {
			return err
		}

		if err := p.sendAll([]byte(item)); err != nil {
			return err
		}
	}

	return nil
}

func (p *Protocol) receivedConfirmation() error {
	// Implement confirmation receiving logic here
	code := make([]byte, 1)
	err := p.receiveAll(code)
	if err != nil {
		return err
	}

	if code[0] != BatchRcvCode {
		return fmt.Errorf("invalid confirmation code received")
	}

	return nil
}

func (p *Protocol) finishBatch() error {
	code := []byte{EndOfBatch}
	if err := p.sendAll(code); err != nil {
		return err
	}
	return nil
}

func (p *Protocol) FinishSendingFilesOf(pattern string) error {
	// Implement finish sending files logic here
	return nil
}

func (p *Protocol) sendAll(data []byte) error {

	len := len(data)

	for sent := 0; sent < len; {
		n, err := p.conn.Write(data[sent:])
		if err != nil {
			return err
		}
		sent += n
	}

	return nil
}

func (p *Protocol) receiveAll(array []byte) error {
	len := len(array)
	received := 0
	for received < int(len) {
		n, err := p.conn.Read(array[received:])
		if err != nil {
			return err
		}
		received += n
	}

	return nil
}

func (p *Protocol) htonsUint32(val uint32) []byte {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, val)
	return bytes
}

func (p *Protocol) ntohsUint32(data []byte) uint32 {
	return binary.BigEndian.Uint32(data)
}
