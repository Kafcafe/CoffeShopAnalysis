package clientHandler

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
	AmountOfDataTypes   = 0x0F
	FilesDataType       = 0x10

	SIZEOF_UINT32 = 4
	SIZEOF_UINT8  = 1
)

type FilesDataTypeData struct {
	DataType string
	Amount   int
}

type Batch struct {
	Lines  []string
	IsLast bool
}

type DataMessage struct {
	OpCode  byte
	Payload any
}

type Protocol struct {
	conn      net.Conn
	log       *logging.Logger
	ackChan   chan bool
	dataChan  chan DataMessage
	errorChan chan error
	isRunning bool
}

// NewProtocol creates a new Protocol instance for the given connection.
// Parameters:
//
//	conn: the network connection
//
// Returns a pointer to the Protocol.
func NewProtocol(conn net.Conn) *Protocol {
	return &Protocol{
		conn:      conn,
		log:       logger.GetLoggerWithPrefix("[PROTO]"),
		ackChan:   make(chan bool, 100),
		dataChan:  make(chan DataMessage, 100),
		errorChan: make(chan error, 100),
		isRunning: true,
	}
}

func (p *Protocol) StartListening() {
	go func() {
		for p.isRunning {
			opCode, err := p.ReceiveOpCode()
			if err != nil {
				p.log.Errorf("Error receiving OpCode: %v", err)
				p.errorChan <- err
				return
			}

			switch opCode {
			case Ack:
				p.handleInboundAck()

			case ReconnectionRequest:
				p.handleInbountReconnectionRequest(opCode)

			case ConnectionRequest, Wait, Begin, ConnectionAccept, ReconnectionAccept, ReconnectionDenied:
				p.dataChan <- DataMessage{OpCode: opCode, Payload: nil}

			case AmountOfDataTypes:
				p.handleAmountOfDataTypes(opCode)

			case FilesDataType:
				p.handleFilesDataType(opCode)

			case MoreBatches:
				p.handleMoreBatches(opCode)

			case EndOfBatch:
				p.handleEndOfBatch(opCode)

			default:
				p.log.Warningf("Unknown OpCode received: %d", opCode)
			}
		}
	}()
}

func (p *Protocol) handleInboundAck() {
	p.ackChan <- true
}

func (p *Protocol) handleInbountReconnectionRequest(opCode byte) {
	clientId, err := p.RcvClientId()
	if err != nil {
		p.log.Errorf("Error receiving ClientId: %v", err)
		p.errorChan <- err
		return
	}
	p.dataChan <- DataMessage{OpCode: opCode, Payload: clientId}
}

func (p *Protocol) handleAmountOfDataTypes(opCode byte) {
	amount, err := p.rcvAmountOfDataTypes()
	if err != nil {
		p.log.Errorf("Error receiving AmountOfDataTypes: %v", err)
		p.errorChan <- err
		return
	}
	p.dataChan <- DataMessage{OpCode: opCode, Payload: amount}
}

func (p *Protocol) handleFilesDataType(opCode byte) {
	dataType, err := p.ReceiveFilesDataType()
	if err != nil {
		p.log.Errorf("Error receiving FilesDataType: %v", err)
		p.errorChan <- err
		return
	}
	amount, err := p.RcvAmountOfFiles()
	if err != nil {
		p.log.Errorf("Error receiving AmountOfFiles: %v", err)
		p.errorChan <- err
		return
	}
	p.dataChan <- DataMessage{OpCode: opCode, Payload: FilesDataTypeData{DataType: dataType, Amount: amount}}
}

func (p *Protocol) handleMoreBatches(opCode byte) {
	lines, isLast, err := p.ReceiveBatch()
	if err != nil {
		p.log.Errorf("Error receiving Batch: %v", err)
		p.errorChan <- err
		return
	}
	p.dataChan <- DataMessage{OpCode: opCode, Payload: Batch{Lines: lines, IsLast: isLast}}
}

func (p *Protocol) handleEndOfBatch(opCode byte) {
	p.dataChan <- DataMessage{OpCode: opCode, Payload: Batch{Lines: []string{}, IsLast: true}}
}

// rcvAmountOfDataTypes receives the number of data types from the connection.
// Returns the amount as int or an error.
func (p *Protocol) rcvAmountOfDataTypes() (amountOfDataTypes int, err error) {
	lenBytes := make([]byte, SIZEOF_UINT32)

	if err := p.receiveAll(lenBytes); err != nil {
		return 0, err
	}

	amount := p.ntohsUint32(lenBytes)

	return int(amount), nil
}

// ReceiveFilesdataType receives the data type string from the connection.
// Returns the data type or an error.
func (p *Protocol) ReceiveFilesDataType() (dataType string, err error) {
	lenBytes := make([]byte, SIZEOF_UINT32)
	if err := p.receiveAll(lenBytes); err != nil {
		return "", err
	}

	dataLen := p.ntohsUint32(lenBytes)

	dataTypeBytes := make([]byte, dataLen)
	if err := p.receiveAll(dataTypeBytes); err != nil {
		return "", err
	}

	return string(dataTypeBytes), nil
}

// receiveLine receives a single line string from the connection.
// Returns the line or an error.
func (p *Protocol) receiveLine() (line string, err error) {
	p.log.Debug("rcv line length")

	lenBytes := make([]byte, SIZEOF_UINT32)
	if err := p.receiveAll(lenBytes); err != nil {
		return "", err
	}

	dataLen := int(p.ntohsUint32(lenBytes))
	p.log.Debugf("rcv line data %v", dataLen)

	lineBytes := make([]byte, dataLen)
	if err := p.receiveAll(lineBytes); err != nil {
		return "", err
	}

	p.log.Debug("line received successfully")
	return string(lineBytes), nil
}

func (p *Protocol) receiveLines(dataLen int) (lines []string, err error) {
	lines = make([]string, dataLen)

	// Loop to receive each line in the batch
	for i := 0; i < dataLen; i++ {
		p.log.Debug("rcv line")
		line, err := p.receiveLine()

		if err != nil {
			return lines, err
		}

		lines[i] = line
	}

	p.log.Debug("batch received successfully")
	return lines, nil
}

// ReceiveBatch receives a batch of lines from the connection.
// Returns the lines, a flag indicating if it's the last batch, and any error.
func (p *Protocol) ReceiveBatch() (lines []string, isLastBatch bool, err error) {
	p.log.Debug("rcv batch data")

	lenBytes := make([]byte, SIZEOF_UINT32)
	if err := p.receiveAll(lenBytes); err != nil {
		return []string{}, false, err
	}

	dataLen := int(p.ntohsUint32(lenBytes))
	p.log.Debugf("rcv batch with %d lines", dataLen)

	lines, err = p.receiveLines(dataLen)

	if err != nil {
		return []string{}, false, err
	}

	return lines, false, nil
}

func (p *Protocol) ReceiveBatchFromChannel() (lines []string, isLastBatch bool, err error) {
	select {
	case msg := <-p.dataChan:
		batch, ok := msg.Payload.(Batch)
		if !ok {
			return nil, false, fmt.Errorf("expected Batch payload, got %T", msg.Payload)
		}
		return batch.Lines, batch.IsLast, nil
	case err := <-p.errorChan:
		return nil, false, err
	}
}

func (p *Protocol) ReceiveAmountOfDataTypesFromChannel() (int, error) {
	select {
	case msg := <-p.dataChan:
		amount, ok := msg.Payload.(int)
		if !ok {
			return 0, fmt.Errorf("expected int payload, got %T", msg.Payload)
		}
		return amount, nil
	case err := <-p.errorChan:
		return 0, err
	}
}

func (p *Protocol) ReceiveFilesDataTypeFromChannel() (string, int, error) {
	select {
	case msg := <-p.dataChan:
		fdt, ok := msg.Payload.(FilesDataTypeData)
		if !ok {
			return "", 0, fmt.Errorf("expected FilesDataTypeData payload, got %T", msg.Payload)
		}
		return fdt.DataType, fdt.Amount, nil
	case err := <-p.errorChan:
		return "", 0, err
	}
}

// ConfirmBatchReceived sends a confirmation code for the received batch.
// Returns an error if sending fails.
func (p *Protocol) ConfirmBatchReceived() error {
	code := []byte{BatchRcvCode}

	if err := p.sendAll(code); err != nil {
		return err
	}

	return nil
}

// rcvAmountOfFiles receives the number of files from the connection.
// Returns the amount as int or an error.
func (p *Protocol) RcvAmountOfFiles() (int, error) {
	lenBytes := make([]byte, SIZEOF_UINT32)
	if err := p.receiveAll(lenBytes); err != nil {
		return 0, err
	}

	amount := p.ntohsUint32(lenBytes)
	return int(amount), nil
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

func (p *Protocol) SendBeginWithClientId(id string) error {
	dataLen := uint32(len(id))
	lenBytes := p.htonsUint32(dataLen)

	packet := make([]byte, 1+4+len(id))
	packet[0] = Begin
	copy(packet[1:], lenBytes)
	copy(packet[5:], []byte(id))

	return p.sendAll(packet)
}

func (p *Protocol) SendResults(query uint32, results []string, isEof bool) error {
	QNumber := p.htonsUint32(query)
	if err := p.sendAll(QNumber); err != nil {
		return err
	}

	if isEof {
		finishQuery := []byte{FinishedQuery}
		if err := p.sendAll(finishQuery); err != nil {
			return err
		}

		p.log.Infof("action: sending end of query %d", query)
		return nil
	} else {
		finishQuery := []byte{NotFinished}
		if err := p.sendAll(finishQuery); err != nil {
			return err
		}
	}

	totalLines := p.htonsUint32(uint32(len(results)))
	if err := p.sendAll(totalLines); err != nil {
		return err
	}

	for _, line := range results {
		lineLenBytes := p.htonsUint32(uint32(len(line)))
		p.log.Debugf("action: sending lenght line | len: %d | query: %d ", len(line), query)
		if err := p.sendAll(lineLenBytes); err != nil {
			return err
		}
		p.log.Debugf("action: sending line | query : %d", query)
		if err := p.sendAll([]byte(line)); err != nil {
			return err
		}
		p.log.Debugf("action: sent line | query: %d | line: %v ", query, line)
	}

	p.log.Debugf("Sent all lines for query ", query)

	return nil

}

// sendAll sends all data over the connection, handling partial writes.
// Parameters:
//
//	data: the byte slice to send
//
// Returns an error if sending fails.
func (p *Protocol) sendAll(data []byte) error {
	len := len(data)

	// Loop to ensure all data is sent
	for sent := 0; sent < len; {
		n, err := p.conn.Write(data[sent:])
		if err != nil {
			return err
		}

		sent += n
	}

	return nil
}

// receiveAll receives all expected data into the array, handling partial reads.
// Parameters:
//
//	array: the byte slice to fill
//
// Returns an error if receiving fails.
func (p *Protocol) receiveAll(array []byte) error {
	len := len(array)
	received := 0

	// Loop to ensure all data is received
	for received < int(len) {
		n, err := p.conn.Read(array[received:])
		if err != nil {
			return err
		}

		received += n
	}

	return nil
}

// htonsUint32 converts a uint32 to big-endian byte array.
// Parameters:
//
//	val: the value to convert
//
// Returns the byte array.
func (p *Protocol) htonsUint32(val uint32) []byte {
	bytes := make([]byte, SIZEOF_UINT32)
	binary.BigEndian.PutUint32(bytes, val)
	return bytes
}

// ntohsUint32 converts a big-endian byte array to uint32.
// Parameters:
//
//	data: the byte array
//
// Returns the uint32 value.
func (p *Protocol) ntohsUint32(data []byte) uint32 {
	return binary.BigEndian.Uint32(data)
}

// Shutdown closes the connection.
// Returns an error if closing fails.
func (p *Protocol) Shutdown() error {
	if p.conn != nil {
		return p.conn.Close()
	}

	return nil
}

func (p *Protocol) ReceiveOpCode() (byte, error) {
	opCode := make([]byte, 1)
	if err := p.receiveAll(opCode); err != nil {
		return 0, err
	}
	return opCode[0], nil
}

func (p *Protocol) ReceiveHandshakeMessage() (DataMessage, error) {
	select {
	case msg := <-p.dataChan:
		return msg, nil
	case err := <-p.errorChan:
		return DataMessage{}, err
	}
}

func (p *Protocol) SendWait() error {
	return p.sendAll([]byte{Wait})
}

func (p *Protocol) SendReconnectionAccept() error {
	return p.sendAll([]byte{ReconnectionAccept})
}

func (p *Protocol) SendReconnectionDenied() error {
	return p.sendAll([]byte{ReconnectionDenied})
}

func (p *Protocol) SendAck() error {
	p.log.Debug("Sent ACK")
	return p.sendAll([]byte{Ack})
}

func (p *Protocol) ReceiveAck() error {
	select {
	case <-p.ackChan:
		return nil
	case err := <-p.errorChan:
		return err
	}
}
