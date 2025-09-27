package clientHandler

import (
	"encoding/binary"
	"net"
)

type Protocol struct {
	conn net.Conn
}

const (
	BatchRcvCode = 0x01
	EndOfBatch   = 0x02
	MoreBatches  = 0x03
)

func NewProtocol(conn net.Conn) *Protocol {
	return &Protocol{
		conn: conn,
	}
}

func (p *Protocol) rcvAmountOfDataTypes() (int, error) {
	lenBytes := make([]byte, 4)
	if err := p.receiveAll(lenBytes); err != nil {
		return 0, err
	}

	amount := p.ntohsUint32(lenBytes)

	return int(amount), nil
}

func (p *Protocol) ReceiveFilesdataType() (string, error) {
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

func (p *Protocol) receiveLine() (string, error) {

	log.Debug("[PROTOCOL] rcv line length")
	lenBytes := make([]byte, 4)
	if err := p.receiveAll(lenBytes); err != nil {
		return "", err
	}

	dataLen := int(p.ntohsUint32(lenBytes))

	log.Debug("[PROTOCOL] rcv line data %v", dataLen)
	data := make([]byte, dataLen)
	if err := p.receiveAll(data); err != nil {
		return "", err
	}

	log.Debug("[PROTOCOL] line received successfully")
	return string(data), nil
}

func (p *Protocol) ReceiveBatch() ([]string, bool, error) {
	endOfBatch := make([]byte, 1)

	log.Debug("[PROTOCOL] rcv end of batch code")
	if err := p.receiveAll(endOfBatch); err != nil {
		return []string{}, false, err
	}

	if endOfBatch[0] == EndOfBatch {
		return []string{}, true, nil
	}

	log.Debug("[PROTOCOL] rcv batch data")
	lenBytes := make([]byte, 4)
	if err := p.receiveAll(lenBytes); err != nil {
		return []string{}, false, err
	}

	dataLen := int(p.ntohsUint32(lenBytes))
	log.Debug("[PROTOCOL] rcv batch with ", dataLen, " lines")
	lines := make([]string, dataLen)
	for i := 0; i < dataLen; i++ {
		log.Debug("[PROTOCOL] rcv line ")
		line, err := p.receiveLine()
		if err != nil {
			return lines, false, err
		}
		lines[i] = line
	}
	log.Debug("[PROTOCOL] batch received successfully")
	return lines, false, nil
}

func (p *Protocol) ConfirmBatchReceived() error {
	code := []byte{BatchRcvCode}
	if err := p.sendAll(code); err != nil {
		return err
	}
	return nil
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
