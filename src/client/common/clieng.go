package client

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/op/go-logging"
)

type Client struct {
	config    *ClientConfig
	protocol  *Protocol
	isRunning bool
	sigChan   chan os.Signal
	currBg    *BatchGenerator
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
		config:    config,
		protocol:  protocol,
		isRunning: true,
		sigChan:   make(chan os.Signal, 1),
		currBg:    nil,
	}

	signal.Notify(client.sigChan, syscall.SIGTERM)
	return client
}

func (c *Client) handleSignals() {
	<-c.sigChan
	c.Shutdown()
}

func (c *Client) Run() ClientExecutionError {
	log.Infof("Client %s is running with server address %s and batch max amount %s",
		c.config.Id,
		c.config.serverAddress,
		c.config.batchMaxAmount,
	)

	var listfiles []string = []string{"transactions", "transaction_items", "stores", "menu", "users"}
	defer c.Shutdown()

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

	err = c.protocol.receivedConfirmation()
	if err != nil {
		log.Error("Error receiving confirmation for batch from file %s: %v", file, err)
		return err
	}

	return nil
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
		close(c.sigChan)
	}

	c.isRunning = false
	log.Info("Client shutdown complete")
}
