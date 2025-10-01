package clientHandler

import (
	logger "common/logger"
	"encoding/binary"
	"net"

	"github.com/op/go-logging"
)

type Protocol struct {
	conn net.Conn
	log  *logging.Logger
}

const (
	BatchRcvCode  = 0x01
	EndOfBatch    = 0x02
	MoreBatches   = 0x03
	FinishedQuery = 0x04
	NotFinished   = 0x05

	SIZEOF_UINT32 = 4
	SIZEOF_UINT8  = 1
)

// NewProtocol creates a new Protocol instance for the given connection.
// Parameters:
//
//	conn: the network connection
//
// Returns a pointer to the Protocol.
func NewProtocol(conn net.Conn) *Protocol {
	return &Protocol{
		conn: conn,
		log:  logger.GetLoggerWithPrefix("[PROTO]"),
	}
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
	p.log.Debug("rcv line data %v", dataLen)

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
func (p *Protocol) ReceiveBatch() (lines []string, isEndOfBatch bool, err error) {
	endOfBatchBytes := make([]byte, SIZEOF_UINT8)

	p.log.Debug("rcv end of batch code")
	if err := p.receiveAll(endOfBatchBytes); err != nil {
		return []string{}, false, err
	}
	endOfBatchCode := endOfBatchBytes[0]

	if endOfBatchCode == EndOfBatch {
		return []string{}, true, nil
	}

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

func (p *Protocol) sendLines(lines []string) error {
	for _, line := range lines {
		lineLenBytes := p.htonsUint32(uint32(len(line)))
		p.log.Debug("[PROTOCOL] Sending line length ", lineLenBytes)
		if err := p.sendAll(lineLenBytes); err != nil {
			return err
		}

		p.log.Debug(" Sending line data")
		if err := p.sendAll([]byte(line)); err != nil {
			return err
		}
	}

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
