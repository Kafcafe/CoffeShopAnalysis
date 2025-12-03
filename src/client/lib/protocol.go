package client

import (
	logger "common/logger"
	"encoding/binary"
	"fmt"
	"net"

	"github.com/op/go-logging"
)

const (
	BatchRcvCode  = 0x01
	EndOfBatch    = 0x02
	MoreBatches   = 0x03
	FinishedQuery = 0x04
	NotFinished   = 0x05
	Start         = 0x06

	ConnectionRequest   = 0x07
	ConnectionAccept    = 0x08
	ReconnectionRequest = 0x09
	ReconnectionAccept  = 0x0A
	ReconnectionDenied  = 0x0B
	Wait                = 0x0C
	Begin               = 0x0D
	Ack                 = 0x0E

	SIZEOF_UINT32 = 4
	SIZEOF_UINT8  = 1
)

type Protocol struct {
	serverAddress      string
	conn               net.Conn
	finishedAllQueries map[int]bool
	log                *logging.Logger
	ackChan            chan bool
}

func NewProtocol(serverAddress string) (*Protocol, error) {
	logger := logger.GetLoggerWithPrefix("[PROTO]")
	conn, err := net.Dial("tcp", serverAddress)

	if err != nil {
		return nil, fmt.Errorf("failed to connect to server at %s", serverAddress)
	}

	return &Protocol{
		serverAddress: serverAddress,
		conn:          conn,
		finishedAllQueries: map[int]bool{
			1: false,
			2: false,
			3: false,
			4: false,
		},
		log:     logger,
		ackChan: make(chan bool),
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

	lenBytes = p.htonsUint32(uint32(amount))
	if err := p.sendAll(lenBytes); err != nil {
		return err
	}

	return nil
}

func (p *Protocol) SendBatch(batch *Batch, batchCount int) error {

	opCode := []byte{MoreBatches}

	p.log.Debug("[PROTOCOL] Sending more batches code")
	if err := p.sendAll(opCode); err != nil {
		return err
	}

	dataLen := uint32(len(batch.Items))
	lenBytes := p.htonsUint32(dataLen)

	p.log.Debugf("[PROTOCOL] Sending batch data for batch %d: %v", lenBytes, batchCount)
	if err := p.sendAll(lenBytes); err != nil {
		return err
	}

	for itemCount, item := range batch.Items {

		itemLenBytes := p.htonsUint32(uint32(len(item)))
		p.log.Debugf("[PROTOCOL] Sending %d item of length %d", itemCount, itemLenBytes)
		if err := p.sendAll(itemLenBytes); err != nil {
			return err
		}

		p.log.Debugf("[PROTOCOL] Sending %d item data", itemCount)
		if err := p.sendAll([]byte(item)); err != nil {
			return err
		}
	}

	p.log.Debugf("[PROTOCOL] Batch %d sent successfully", batchCount)
	return nil
}

func (p *Protocol) Listen() (queryCode uint32, lines []string, finish bool, err error, finishedAll bool) {
	for {
		// Determine message type
		msgType := make([]byte, 1)
		if err := p.receiveAll(msgType); err != nil {
			p.log.Error("Error receiving message type: %v", err)
			return 0, nil, true, err, false
		}

		if msgType[0] == Ack {
			p.log.Debug("Received ACK in Listen loop")
			p.ackChan <- true
			continue
		}

		// If not ACK then wait for query number
		queryNumber, err := p.readQueryNumber(msgType)
		if err != nil {
			p.log.Error("Error receiving QNumber remaining bytes: %v", err)
			return 0, nil, true, err, false
		}
		p.log.Debug("Received queryNumber: ", queryNumber)

		isFinished, err := p.readFinishQueryStatus()
		if err != nil {
			p.log.Error("Error receiving FinishedQuery code: %v", err)
			return 0, nil, true, err, false
		}

		if isFinished {
			allFinished := p.markQueryFinished(queryNumber)
			return queryNumber, nil, true, nil, allFinished
		}

		lines, err = p.readQueryResults()
		if err != nil {
			return 0, nil, true, err, false
		}

		p.log.Debug("Finished receiving all lines for query ", queryNumber)

		return queryNumber, lines, false, nil, false
	}
}

func (p *Protocol) readQueryNumber(firstByte []byte) (uint32, error) {
	queryNumberRemaining := make([]byte, 3)
	if err := p.receiveAll(queryNumberRemaining); err != nil {
		return 0, err
	}

	// Combine first byte and remaining bytes
	queryNumberBytes := append(firstByte, queryNumberRemaining...)
	return p.ntohsUint32(queryNumberBytes), nil
}

func (p *Protocol) readFinishQueryStatus() (bool, error) {
	finishQuery := make([]byte, SIZEOF_UINT8)

	if err := p.receiveAll(finishQuery); err != nil {
		return false, err
	}

	return finishQuery[0] == FinishedQuery, nil
}

func (p *Protocol) markQueryFinished(queryNumber uint32) bool {
	p.finishedAllQueries[int(queryNumber)] = true
	p.log.Debug("[CLIENT-P] | action: receive query end | query:", queryNumber)

	return p.finishedAllQueries[1] && p.finishedAllQueries[2] && p.finishedAllQueries[3] && p.finishedAllQueries[4]
}

func (p *Protocol) readQueryResults() ([]string, error) {
	totalLines := make([]byte, 4)

	if err := p.receiveAll(totalLines); err != nil {
		p.log.Error("Error receiving totalLines: %v", err)
		return nil, err
	}

	totalLinesBytes := int(p.ntohsUint32(totalLines))
	p.log.Debug("[CLIENT-P] Received totalLines: ", totalLinesBytes)

	lines := make([]string, totalLinesBytes)

	for i := 0; i < totalLinesBytes; i++ {
		lineLen := make([]byte, SIZEOF_UINT32)
		if err := p.receiveAll(lineLen); err != nil {
			p.log.Error("Error receiving line length: %v", err)
			return nil, err
		}

		lineLenBytes := int(p.ntohsUint32(lineLen))
		p.log.Debug("Received line length: ", lineLenBytes)

		lineData := make([]byte, lineLenBytes)
		if err := p.receiveAll(lineData); err != nil {
			p.log.Error("Error receiving line data: %v", err)
			return nil, err
		}
		lines[i] = string(lineData)
		p.log.Debug("Received line data: ", string(lineData))
	}

	return lines, nil
}

func (p *Protocol) SendReconnectionRequest(id string) error {
	dataLen := uint32(len(id))
	lenBytes := p.htonsUint32(dataLen)

	packet := make([]byte, 1+4+len(id))
	packet[0] = ReconnectionRequest
	copy(packet[1:], lenBytes)
	copy(packet[5:], []byte(id))

	return p.sendAll(packet)
}

func (p *Protocol) finishBatch() error {
	code := []byte{EndOfBatch}
	if err := p.sendAll(code); err != nil {
		return err
	}
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

func (p *Protocol) RcvClientId() (string, error) {
	lenBytes := make([]byte, SIZEOF_UINT32)
	if err := p.receiveAll(lenBytes); err != nil {
		return "", err
	}

	dataLen := p.ntohsUint32(lenBytes)

	idBytes := make([]byte, dataLen)
	if err := p.receiveAll(idBytes); err != nil {
		return "", err
	}

	return string(idBytes), nil
}

func (p *Protocol) Shutdown() error {
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}

func (p *Protocol) ReceiveHandshakeResponse() (byte, error) {
	resp := make([]byte, 1)
	for {
		if err := p.receiveAll(resp); err != nil {
			return 0, err
		}

		if resp[0] == Wait || resp[0] == Begin {
			return resp[0], nil
		}
		p.log.Warningf("Ignored unexpected byte: 0x%x", resp[0])
	}
}

func (p *Protocol) ReceiveReconnectionResponse() (byte, error) {
	resp := make([]byte, 1)
	if err := p.receiveAll(resp); err != nil {
		return 0, err
	}
	return resp[0], nil
}

func (p *Protocol) ReceiveAck() error {
	<-p.ackChan
	return nil
}
