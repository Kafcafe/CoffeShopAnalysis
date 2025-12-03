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

func (p *Protocol) Listen() (QueryCod uint32, lines []string, finish bool, err error, finishedAll bool) {
	// Read first byte to determine message type
	msgType := make([]byte, 1)
	if err := p.receiveAll(msgType); err != nil {
		p.log.Error("Error receiving message type: %v", err)
		return 0, nil, true, err, false
	}

	if msgType[0] == Ack {
		p.log.Debug("Received ACK in Listen loop")
		p.ackChan <- true
		// Continue listening (recursive call or return special value to indicate "continue")
		// Since we need to return something, we can return a special error or handle this loop in the caller.
		// Better approach: The caller (ProcessResults) should loop.
		// We return a special "AckReceived" state or similar?
		// Actually, ProcessResults expects Results. If we get an ACK, we handle it and continue listening.
		// But ReceiveAck is waiting for this.
		// So we should just continue listening here? No, this function returns ONE message.
		// If it's an ACK, we shouldn't return it as a Result.
		// We should probably loop INSIDE Listen until we get a Result or Error.
		return p.Listen()
	}

	// If not ACK, it must be a Result (starting with QNumber)
	// The first byte we read is the first byte of QNumber (uint32).
	// We need to read the remaining 3 bytes of QNumber.
	QNumberRemaining := make([]byte, 3)
	if err := p.receiveAll(QNumberRemaining); err != nil {
		p.log.Error("Error receiving QNumber remaining bytes: %v", err)
		return 0, nil, true, err, false
	}

	// Combine first byte and remaining bytes
	QNumber := append(msgType, QNumberRemaining...)
	qNumber := p.ntohsUint32(QNumber)
	p.log.Debug("Received QNumber: ", qNumber)

	finishQuery := make([]byte, SIZEOF_UINT8)

	if err := p.receiveAll(finishQuery); err != nil {
		p.log.Error("Error sending FinishedQuery code: %v", err)
		return 0, nil, true, err, false
	}

	if finishQuery[0] == FinishedQuery {
		p.finishedAllQueries[int(qNumber)] = true
		p.log.Debug("[CLIENT-P] | action: receive query end | query:", qNumber)

		if p.finishedAllQueries[1] && p.finishedAllQueries[2] && p.finishedAllQueries[3] && p.finishedAllQueries[4] {
			return qNumber, nil, true, nil, true
		}
		return qNumber, nil, true, nil, false
	}

	totalLines := make([]byte, 4)

	if err := p.receiveAll(totalLines); err != nil {
		p.log.Error("Error receiving totalLines: %v", err)
		return 0, nil, true, err, false
	}

	totalLinesBytes := int(p.ntohsUint32(totalLines))
	p.log.Debug("[CLIENT-P] Received totalLines: ", totalLinesBytes)

	lines = make([]string, totalLinesBytes)

	for i := 0; i < totalLinesBytes; i++ {
		lineLen := make([]byte, SIZEOF_UINT32)
		if err := p.receiveAll(lineLen); err != nil {
			p.log.Error("Error receiving line length: %v", err)
			return 0, nil, true, err, false
		}

		lineLenBytes := int(p.ntohsUint32(lineLen))
		p.log.Debug("Received line length: ", lineLenBytes)

		lineData := make([]byte, lineLenBytes)
		if err := p.receiveAll(lineData); err != nil {
			p.log.Error("Error receiving line data: %v", err)
			return 0, nil, true, err, false
		}
		lines[i] = string(lineData)
		p.log.Debug("Received line data: ", string(lineData))
	}

	p.log.Debug("Finished receiving all lines for query ", qNumber)

	return qNumber, lines, false, nil, false
}

// func (p *Protocol) receivedConfirmation() error {
// 	code := make([]byte, 1)
// 	err := p.receiveAll(code)
// 	if err != nil {
// 		return err
// 	}

// 	if code[0] != BatchRcvCode {
// 		return fmt.Errorf("invalid confirmation code received")
// 	}

// 	return nil
// }

func (p *Protocol) sendClientId(id string) error {
	dataLen := uint32(len(id))
	lenBytes := p.htonsUint32(dataLen)

	if err := p.sendAll(lenBytes); err != nil {
		return err
	}

	if err := p.sendAll([]byte(id)); err != nil {
		return err
	}

	return nil
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

func (p *Protocol) FinishSendingFilesOf(pattern string) error {
	// Implement finish sending files logic here
	return nil
}

func (p *Protocol) rcvStart() error {
	start := make([]byte, SIZEOF_UINT8)

	p.log.Debug("rcv start code")
	if err := p.receiveAll(start); err != nil {
		return err
	}
	startCode := start[0]

	if startCode != Start {
		return fmt.Errorf("invalid start code received")
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
