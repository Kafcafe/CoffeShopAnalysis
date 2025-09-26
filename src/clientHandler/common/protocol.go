package clientHandler

import (
	"encoding/binary"
	"net"
)

type Protocol struct {
	conn net.Conn
}

func NewProtocol(conn net.Conn) *Protocol {
	return &Protocol{
		conn: conn,
	}
}

func (p *Protocol) rcvAmountOfTopics() (int, error) {
	lenBytes := make([]byte, 4)
	if err := p.receiveAll(lenBytes); err != nil {
		return 0, err
	}

	amount := p.ntohsUint32(lenBytes)

	return int(amount), nil
}

func (p *Protocol) ReceiveFilesTopic() (string, error) {
	// Implement file topic receiving logic here
	lenBytes := make([]byte, 4)
	if err := p.receiveAll(lenBytes); err != nil {
		return "", err
	}
	dataLen := p.ntohsUint32(lenBytes)

	data := make([]byte, dataLen)
	if err := p.receiveAll(data); err != nil {
		return "", err
	}

	return string(data), nil
}

func (p *Protocol) rcvAmountOfFiles() (int, error) {
	lenBytes := make([]byte, 4)
	if err := p.receiveAll(lenBytes); err != nil {
		return 0, err
	}

	amount := p.ntohsUint32(lenBytes)
	return int(amount), nil
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
