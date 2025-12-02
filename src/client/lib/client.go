package client

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	logger "common/logger"

	atomicwritter "common/atomic_writter"

	"github.com/op/go-logging"
)

type Client struct {
	config       *ClientConfig
	protocol     *Protocol
	isRunning    bool
	sigChan      chan os.Signal
	currBg       *BatchGenerator
	Id           string
	results      map[int][]string
	finishedChan chan bool
	fileTypes    string
	log          *logging.Logger

	was_signaled bool
	mutex        sync.Mutex
	atomicWriter *atomicwritter.AtomicWriter
	sessionId    string
}

type ClientExecutionError error

func NewClient(config *ClientConfig, clientId, fileTypes string) *Client {
	protocol, err := NewProtocol(config.serverAddress)
	logger := logger.GetLoggerWithPrefix("[CLIENT]")

	if err != nil {
		logger.Error("Error connecting to server: %v", err)
		return nil
	}

	prefix := fmt.Sprintf("client%s", clientId)
	persistancePath := fmt.Sprintf("sent_data/%s", prefix)

	client := &Client{
		config:       config,
		protocol:     protocol,
		isRunning:    true,
		sigChan:      make(chan os.Signal, 1),
		currBg:       nil,
		results:      make(map[int][]string),
		finishedChan: make(chan bool, 1),
		log:          logger,
		Id:           clientId,
		fileTypes:    fileTypes,
		was_signaled: false,
		mutex:        sync.Mutex{},
		atomicWriter: atomicwritter.NewAtomicWriter(persistancePath),
		sessionId:    "",
	}

	signal.Notify(client.sigChan, syscall.SIGTERM)
	return client
}

func (c *Client) handleSignals() {
	<-c.sigChan

	c.log.Info("Received shutdown signal")

	c.mutex.Lock()
	c.was_signaled = true
	c.mutex.Unlock()

	c.Shutdown()
}

func (c *Client) return_err_if_not_signaled(err error) error {
	var was_signaled bool

	c.mutex.Lock()
	was_signaled = c.was_signaled
	c.mutex.Unlock()

	if was_signaled {
		return nil
	} else {
		c.Shutdown()
		return err
	}
}

func (c *Client) Run() ClientExecutionError {
	c.log.Infof("| action: run client | client_id: %s | server_address: %s | batch_max_amount: %d",
		c.Id,
		c.config.serverAddress,
		c.config.batchMaxAmount,
	)

	var listfiles []string = strings.Split(c.fileTypes, ",")
	c.log.Info(listfiles)
	go c.handleSignals()

	fileHandler := NewFileHandler(c.config.dataPath)

	c.log.Info("Sleeping...")
	time.Sleep(time.Duration(15) * time.Second)
	c.log.Info("Woke up")

	err := c.performHandshake()
	if err != nil {
		c.log.Errorf("| action: Handshake failed: %v | result: error", err)
		return c.return_err_if_not_signaled(err)
	}

	//go c.ProcessResults()

	err = c.protocol.sendAmountOfTopics(len(listfiles))
	if err != nil {
		c.log.Errorf("| action: Error sending amount of topics: %v | result: error", err)
		return c.return_err_if_not_signaled(err)
	}

	err = c.protocol.ReceiveAck()
	if err != nil {
		return fmt.Errorf("error receiving ACK after sendAmountOfTopics: %v", err)
	}

	for _, pattern := range listfiles {
		files, err := c.GetFilesWithPattern(pattern, fileHandler)
		if err != nil {
			c.log.Errorf("| action: Error getting files: %v | result: error", err)
			return c.return_err_if_not_signaled(err)
		}

		if err = c.protocol.SendFilesTopic(pattern, len(files)); err != nil {
			c.log.Errorf("| action: Error sending files topic: %v | result: error", err)
			return c.return_err_if_not_signaled(err)
		}

		err = c.protocol.ReceiveAck()
		if err != nil {
			return fmt.Errorf("error receiving ACK after SendFilesTopic: %v", err)
		}

		if err = c.ProcessFileList(files, pattern); err != nil {
			c.log.Errorf("| action: Error processing file list: %v | result: error", err)
			return c.return_err_if_not_signaled(err)
		}
	}

	c.atomicWriter.CleanAll()

	<-c.finishedChan
	c.Shutdown()
	return nil
}

func (c *Client) performHandshake() error {
	sessionId, lastFileProcessed := c.attemptRecovery()
	reconnected := false
	var err error = nil

	shouldReconnect := sessionId != "" && lastFileProcessed != ""
	if shouldReconnect {
		reconnected, err = c.attemptReconnection(sessionId)
		if err != nil {
			c.log.Warningf("Reconnection attempt failed: %v. Proceeding as new connection", err)
		}
	}

	if reconnected {
		c.log.Info("Reconnection successful")
		return nil
	}

	return c.startNewConnection()
}

func (c *Client) startNewConnection() error {
	c.log.Info("Starting new connection")
	err := c.protocol.sendAll([]byte{ConnectionRequest})
	if err != nil {
		return err
	}

	resp, err := c.protocol.ReceiveHandshakeResponse()
	if err != nil {
		return err
	}

	switch resp {
	case Wait:
		c.log.Info("Server is full. Waiting...")
		// Wait for BEGIN
		resp, err = c.protocol.ReceiveHandshakeResponse()
		if err != nil {
			return err
		}
		if resp != Begin {
			return fmt.Errorf("unexpected response after WAIT: %x", resp)
		}

	case Begin:
		c.log.Info("Connection accepted (BEGIN received)")

		// Receive Session ID
		sessionId, err := c.protocol.RcvClientId()
		if err != nil {
			return fmt.Errorf("error receiving Session ID: %v", err)
		}
		c.sessionId = sessionId
		c.log.Infof("Assigned Session ID: %s", c.sessionId)
		return nil
	default:
		return fmt.Errorf("unexpected response during handshake: %x", resp)
	}

	return nil
}

func (c *Client) attemptRecovery() (sessionId, lastFileProcessed string) {
	c.log.Info("Attempting recovery")

	savedData, err := c.atomicWriter.Recover()
	if err != nil {
		c.log.Warningf("Could not recover state because: %v. Proceeding as new connection", err)
		return "", ""
	}

	sessionId = ""
	lastFileProcessed = ""
	if savedData != nil {
		sessionData, ok := savedData["session"]
		if ok {
			sessionId, lastFileProcessed = c.validateRecoveredData(sessionData)
		} else {
			c.log.Info("No state from previous sessions found. Starting clean")
		}
	}

	return sessionId, lastFileProcessed
}

func (c *Client) validateRecoveredData(data *atomicwritter.SavedInfo) (sessionId, lastFileProcessed string) {
	sessionId = data.GetDataType()

	if len(sessionId) > 0 {
		c.log.Infof("Recovered session ID: %s", sessionId)
	} else {
		c.log.Info("Could not recover session ID")
	}

	dataString := data.GetData()
	if len(dataString) > 0 {
		lastFileProcessed = dataString[0]
		c.log.Infof("Recovered last file processed: %s", lastFileProcessed)
	} else {
		c.log.Info("Could not recover last file processed")
	}

	return sessionId, lastFileProcessed
}

func (c *Client) attemptReconnection(sessionId string) (reconnected bool, err error) {
	reconnected = false

	c.log.Info("Attempting ReconnectionRequest")
	err = c.protocol.SendReconnectionRequest(sessionId)
	if err != nil {
		return reconnected, err
	}

	resp, err := c.protocol.ReceiveReconnectionResponse()
	if err != nil {
		return reconnected, err
	}

	switch resp {
	case ReconnectionAccept:
		reconnected = true
		c.log.Info("Reconnection accepted")
		return reconnected, nil
	case ReconnectionDenied:
		c.log.Info("Reconnection denied. Attempting new ConnectionRequest")
	default:
		return reconnected, fmt.Errorf("unexpected response to ReconnectionRequest: %d", resp)
	}

	return reconnected, nil
}

func (c *Client) GetFilesWithPattern(pattern string, fh *FileHandler) ([]string, error) {
	c.log.Infof("| action: get files with pattern | client_id: %s | pattern: %s", c.Id, pattern)
	files, err := fh.GetFilesWithPattern(pattern)

	if err != nil {
		return nil, fmt.Errorf("| action: Error getting files with pattern %s: %v | result: error", pattern, err)
	}

	return files, nil
}

func (c *Client) ProcessFileList(files []string, pattern string) error {
	for _, file := range files {
		c.log.Infof("| action: process file | client_id: %s | file: %s", c.Id, file)

		c.currBg = NewBatchGenerator(c.config.dataPath, file)
		if c.currBg == nil {
			return fmt.Errorf("| action: Error creating batch generator for file %s | result: error", file)
		}

		batchCount := 1

		for c.currBg.IsReading() {
			if err := c.processBatch(c.currBg, file, batchCount); err != nil {
				return fmt.Errorf("| action: Error processing batch %d for file %s: %v | result: error", batchCount, file, err)
			}
			batchCount += 1
		}

		err := c.protocol.finishBatch()
		if err != nil {
			return fmt.Errorf("| action: Error finishing batch for file %s: %v | result: error", file, err)
		}

		err = c.protocol.ReceiveAck()
		if err != nil {
			return fmt.Errorf("error receiving ACK after finishBatch: %v", err)
		}

		c.atomicWriter.WriteLine(fmt.Sprintf("%s", file), ".txt", []string{"session", c.sessionId})
		c.log.Infof("| action: Finished processing file | client_id: %s | file: %s", c.Id, file)
	}

	err := c.protocol.FinishSendingFilesOf(pattern)

	if err != nil {
		return fmt.Errorf("| action: Error finishing sending files of pattern %s: %v | result: error", pattern, err)
	}
	return nil
}

func (c *Client) processBatch(bg *BatchGenerator, file string, batchCount int) error {
	batch, err := bg.GetNextBatch(c.config.batchMaxAmount)
	if err != nil {
		return fmt.Errorf("| action: Error getting next batch from file %s: %v | result: error", file, err)
	}

	err = c.protocol.SendBatch(batch, batchCount)
	if err != nil {
		return fmt.Errorf("| action: Error sending batch from file %s: %v | result: error", file, err)
	}

	if err := c.protocol.ReceiveAck(); err != nil {
		return fmt.Errorf("error receiving ACK after SendBatch: %v", err)
	}

	c.log.Debugf("Sent batch %d", batchCount)
	return nil
}

func (c *Client) ProcessResults() error {
	for c.isRunning {
		query, lines, finish, err, finishedAll := c.protocol.rcvResults()

		if err != nil {
			c.log.Errorf("action: Error receiving results: %v, result: error", err)
		}

		if finish && !finishedAll {
			c.log.Infof("Finished receiving results for query %d | results: %v", query, len(c.results[int(query)]))
			c.LogFinishQuery(int(query))
			continue
		} else if finish && finishedAll {
			c.log.Debug("Finished receiving results for query %d", query)
			c.LogFinishQuery(int(query))
			c.finishedChan <- true
			return nil
		}

		c.log.Debugf("[CLIENT] | action: received results for query %d | results: %s | of len: %d", query, strings.Join(lines, ", "), len(lines))

		c.results[int(query)] = append(c.results[int(query)], lines...)
		c.log.Debug(c.results)
	}
	return nil
}

func (c *Client) LogFinishQuery(query int) {
	if query <= 0 || query >= 5 {
		return
	}

	c.log.Infof("| action: Finished receiving results for query %d", query)
	savePath := fmt.Sprintf("./results/results_q%d_%s.txt", query, c.Id)

	if err := WriteLines(c.results[query], savePath); err != nil {
		c.log.Errorf("| action: Error writing results for query %d: %v", query, err)
	}
}

// WriteLines overwrites the file at filePath with the given lines,
// creating parent directories if needed.
func WriteLines(lines []string, filePath string) error {
	// Ensure parent directory exists

	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return fmt.Errorf("| action: Error while creating results folder %v", err)
	}

	// Join lines with newline separator
	content := strings.Join(lines, "\n")

	// Write or overwrite the file
	return os.WriteFile(filePath, []byte(content), 0644)
}

func (c *Client) Shutdown() {
	if c.protocol != nil {
		c.protocol.Shutdown()
	}

	if c.currBg != nil {
		c.currBg.Close()
	}

	if c.sigChan != nil {
		signal.Stop(c.sigChan)
	}

	c.isRunning = false
	c.log.Info("Client shutdown complete")
}
