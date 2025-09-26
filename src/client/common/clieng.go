package client

import (
	"fmt"

	"github.com/op/go-logging"
)

type Client struct {
	config    *ClientConfig
	protocol  *Protocol
	isRunning bool
}

var log = logging.MustGetLogger("log")

// ClientExecutionError represents an error during client execution.
type ClientExecutionError error

func NewClient(config *ClientConfig) *Client {
	protocol, err := NewProtocol(config.serverAddress)

	if err != nil {
		log.Errorf("Error connecting to server: %v", err)
		return nil
	}

	return &Client{
		config:    config,
		protocol:  protocol,
		isRunning: true,
	}
}

func (c *Client) Run() ClientExecutionError {
	log.Infof("Client %s is running with server address %s and batch max amount %s",
		c.config.Id,
		c.config.serverAddress,
		c.config.batchMaxAmount,
	)

	var listfiles []string = []string{"transactions", "transaction_items", "stores", "menu", "users"}

	fileHandler := NewFileHandler(c.config.dataPath)

	err := c.protocol.sendAmountOfTopics(len(listfiles))

	if err != nil {
		log.Errorf("Error sending amount of topics: %v", err)
		return err
	}

	for _, pattern := range listfiles {
		files, err := c.GetFilesWithPattern(pattern, fileHandler)
		if err != nil {
			log.Errorf("Error getting files: %v", err)
			return err
		}

		if err = c.protocol.SendFilesTopic(pattern, len(files)); err != nil {
			log.Errorf("Error sending files topic: %v", err)
			return err
		}

		if err = c.ProcessFileList(files, pattern); err != nil {
			log.Errorf("Error processing file list: %v", err)
			return err
		}
	}

	return nil
}

func (c *Client) GetFilesWithPattern(pattern string, fh *FileHandler) ([]string, error) {
	log.Infof("Processing files with pattern: %s", pattern)
	files, err := fh.GetFilesWithPattern(pattern)

	if err != nil {
		log.Errorf("Error getting files with pattern %s: %v", pattern, err)
		return nil, err
	}

	return files, nil
}

func (c *Client) ProcessFileList(files []string, pattern string) error {
	for _, file := range files {
		log.Infof("Processing file: %s", file)

		batchGenerator := NewBatchGenerator(c.config.dataPath, file)

		if batchGenerator == nil {
			log.Errorf("Error creating batch generator for file %s", file)
			return fmt.Errorf("error creating batch generator for file %s", file)
		}

		for batchGenerator.IsReading() {
			if err := c.processBatch(batchGenerator, file); err != nil {
				log.Errorf("Error processing batch for file %s: %v", file, err)
				return err
			}
		}
	}

	err := c.protocol.FinishSendingFilesOf(pattern)

	if err != nil {
		log.Errorf("Error finishing sending files of pattern %s: %v", pattern, err)
		return err
	}
	return nil

}

func (c *Client) processBatch(bg *BatchGenerator, file string) error {

	batch, err := bg.GetNextBatch(c.config.batchMaxAmount)

	if err != nil {
		log.Errorf("Error getting next batch from file %s: %v", file, err)
		return err
	}

	err = c.protocol.SendBatch(batch)

	if err != nil {
		log.Errorf("Error sending batch from file %s: %v", file, err)
		return err
	}

	log.Infof("Sent batch with information of file: %s", file)

	err = c.protocol.receivedConfirmation()
	if err != nil {
		log.Errorf("Error receiving confirmation for batch from file %s: %v", file, err)
		return err
	}

	return nil
}
