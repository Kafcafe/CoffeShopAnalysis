package client

import (
	"encoding/binary"
	"fmt"
	"net"
)

const (
	BatchRcvCode  = 0x01
	EndOfBatch    = 0x02
	MoreBatches   = 0x03
	FinishedQuery = 0x04
	NotFinished   = 0x05

	SIZEOF_UINT32 = 4
	SIZEOF_UINT8  = 1
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

	lenBytes = p.htonsUint32(uint32(amount))
	if err := p.sendAll(lenBytes); err != nil {
		return err
	}

	return nil
}

func (p *Protocol) SendBatch(batch *Batch) error {

	opCode := []byte{MoreBatches}

	log.Debug("[PROTOCOL] Sending more batches code")
	if err := p.sendAll(opCode); err != nil {
		return err
	}

	dataLen := uint32(len(batch.Items))
	lenBytes := p.htonsUint32(dataLen)

	log.Debug("[PROTOCOL] Sending batch data", lenBytes)
	if err := p.sendAll(lenBytes); err != nil {
		return err
	}

	for _, item := range batch.Items {

		itemLenBytes := p.htonsUint32(uint32(len(item)))
		log.Debug("[PROTOCOL] Sending item of length ", itemLenBytes)
		if err := p.sendAll(itemLenBytes); err != nil {
			return err
		}

		log.Debug("[PROTOCOL] Sending item data")
		if err := p.sendAll([]byte(item)); err != nil {
			return err
		}
	}

	log.Debug("[PROTOCOL] Batch sent successfully")
	return nil
}

func (p *Protocol) rcvResults() (QueryCod uint32, lines []string, finish bool, err error) {
	log.Debug("[CLIENT-P] Receiving results...")
	QNumber := make([]byte, SIZEOF_UINT32)
	if err := p.receiveAll(QNumber); err != nil {
		log.Error("Error receiving QNumber: %v", err)
		return 0, nil, true, err
	}

	qNumber := p.ntohsUint32(QNumber)
	log.Debug("Received QNumber: ", qNumber)

	finishQuery := make([]byte, SIZEOF_UINT8)

	if err := p.receiveAll(finishQuery); err != nil {
		log.Error("Error sending FinishedQuery code: %v", err)
		return 0, nil, true, err
	}

	if finishQuery[0] == FinishedQuery {
		log.Debug("[CLIENT-P] | action: receive query end | query:", qNumber)
		return qNumber, nil, true, nil
	}

	totalLines := make([]byte, 4)

	if err := p.receiveAll(totalLines); err != nil {
		log.Error("Error receiving totalLines: %v", err)
		return 0, nil, true, err
	}

	totalLinesBytes := int(p.ntohsUint32(totalLines))
	log.Debug("[CLIENT-P] Received totalLines: ", totalLinesBytes)

	lines = make([]string, totalLinesBytes)

	for i := 0; i < totalLinesBytes; i++ {
		lineLen := make([]byte, SIZEOF_UINT32)
		if err := p.receiveAll(lineLen); err != nil {
			log.Error("Error receiving line length: %v", err)
			return 0, nil, true, err
		}

		lineLenBytes := int(p.ntohsUint32(lineLen))
		log.Debug("Received line length: ", lineLenBytes)

		lineData := make([]byte, lineLenBytes)
		if err := p.receiveAll(lineData); err != nil {
			log.Error("Error receiving line data: %v", err)
			return 0, nil, true, err
		}
		lines[i] = string(lineData)
		log.Debug("Received line data: ", string(lineData))
	}

	log.Debug("Finished receiving all lines for query ", qNumber)

	return qNumber, lines, false, nil
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

func (p *Protocol) Shutdown() error {
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}
