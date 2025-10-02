package client

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

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
}

var log = logging.MustGetLogger("log")

type ClientExecutionError error

func NewClient(config *ClientConfig) *Client {
	protocol, err := NewProtocol(config.serverAddress)

	if err != nil {
		log.Error("Error connecting to server: %v", err)
		return nil
	}

	client := &Client{
		config:       config,
		protocol:     protocol,
		isRunning:    true,
		sigChan:      make(chan os.Signal, 1),
		currBg:       nil,
		results:      make(map[int][]string),
		finishedChan: make(chan bool, 1),
	}

	signal.Notify(client.sigChan, syscall.SIGTERM)
	return client
}

func (c *Client) handleSignals() {
	<-c.sigChan
	c.Shutdown()
}

func (c *Client) Run() ClientExecutionError {
	log.Infof("Client %s is running with server address %s and batch max amount %d",
		c.config.serverAddress,
		c.config.batchMaxAmount,
	)

	var fileTypes string = os.Getenv("FILETYPES")
	c.Id = os.Getenv("ID")
	var listfiles []string = strings.Split(fileTypes, ",")
	log.Info(listfiles)
	defer c.Shutdown()
	go c.handleSignals()
	go c.ProcessResults()

	fileHandler := NewFileHandler(c.config.dataPath)

	err := c.protocol.sendAmountOfTopics(len(listfiles))

	if err != nil {
		log.Error("Error sending amount of topics: %v", err)
		return err
	}

	for _, pattern := range listfiles {
		files, err := c.GetFilesWithPattern(pattern, fileHandler)
		if err != nil {
			log.Error("Error getting files: %v", err)
			return err
		}

		if err = c.protocol.SendFilesTopic(pattern, len(files)); err != nil {
			log.Error("Error sending files topic: %v", err)
			return err
		}

		if err = c.ProcessFileList(files, pattern); err != nil {
			log.Error("Error processing file list: %v", err)
			return err
		}
	}

	<-c.finishedChan

	return nil
}

func (c *Client) GetFilesWithPattern(pattern string, fh *FileHandler) ([]string, error) {
	log.Infof("Processing files with pattern: %s", pattern)
	files, err := fh.GetFilesWithPattern(pattern)

	if err != nil {
		log.Error("Error getting files with pattern %s: %v", pattern, err)
		return nil, err
	}

	return files, nil
}

func (c *Client) ProcessFileList(files []string, pattern string) error {
	for _, file := range files {
		log.Infof("Processing file: %s", file)

		c.currBg = NewBatchGenerator(c.config.dataPath, file)

		if c.currBg == nil {
			log.Error("Error creating batch generator for file %s", file)
			return fmt.Errorf("error creating batch generator for file %s", file)
		}

		for c.currBg.IsReading() {
			if err := c.processBatch(c.currBg, file); err != nil {
				log.Error("Error processing batch for file %s: %v", file, err)
				return err
			}
			log.Info("Sending Batch...")
		}

		err := c.protocol.finishBatch()

		if err != nil {
			log.Error("Error finishing batch for file %s: %v", file, err)
			return err
		}

		log.Infof("Finished processing file: %s", file)

	}

	err := c.protocol.FinishSendingFilesOf(pattern)

	if err != nil {
		log.Error("Error finishing sending files of pattern %s: %v", pattern, err)
		return err
	}
	return nil

}

func (c *Client) processBatch(bg *BatchGenerator, file string) error {

	batch, err := bg.GetNextBatch(c.config.batchMaxAmount)

	if err != nil {
		log.Error("Error getting next batch from file %s: %v", file, err)
		return err
	}

	err = c.protocol.SendBatch(batch)

	if err != nil {
		log.Error("Error sending batch from file %s: %v", file, err)
		return err
	}

	log.Infof("Sent batch with information of file: %s", file)

	return nil
}

func (c *Client) ProcessResults() error {
	for c.isRunning {
		query, lines, finish, err, finishedAll := c.protocol.rcvResults()

		if err != nil {
			log.Error("Error receiving results: %v", err)
		}

		if finish {
			log.Debug("Finished receiving results for query %d", query)
			c.LogFinishQuery(int(query))
			continue
		}

		if finishedAll {
			log.Info("Finished all queries")
			c.finishedChan <- true
			return nil
		}

		log.Debugf("[CLIENT] | action: received results for query %d | results: %s | of len: %d", query, strings.Join(lines, ", "), len(lines))

		c.results[int(query)] = append(c.results[int(query)], lines...)
		log.Debug(c.results)
	}
	return nil
}

func (c *Client) LogFinishQuery(query int) {
	if query <= 0 || query >= 5 {
		return
	}

	log.Infof("Finished receiving results for query %d", query)
	log.Info("Results:", c.results[query])

	savePath := fmt.Sprintf("./results/results_q%d.txt", query)
	WriteLines(c.results[query], savePath)
}

// WriteLines overwrites the file at filePath with the given lines,
// creating parent directories if needed.
func WriteLines(lines []string, filePath string) error {
	// Ensure parent directory exists

	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return err
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
	log.Info("Client shutdown complete")
}
