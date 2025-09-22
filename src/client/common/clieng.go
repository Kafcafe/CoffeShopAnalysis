package client

import (
	"bufio"
	"fmt"
	"os"

	"github.com/op/go-logging"
)

type Client struct {
	config *ClientConfig
}

var log = logging.MustGetLogger("log")

// ClientExecutionError represents an error during client execution.
type ClientExecutionError error

func NewClient(config *ClientConfig) *Client {
	return &Client{
		config: config,
	}
}

func (c *Client) Run() ClientExecutionError {
	log.Infof("Client %s is running with server address %s and batch max amount %s",
		c.config.Id,
		c.config.serverAddress,
		c.config.batchMaxAmount,
	)
	// Implement the main logic of the client here.
	// This is a placeholder for demonstration purposes.
	filepath := "./data/transactions.csv"

	log.Infof("Processing file: %s", filepath)

	file, err := os.Open(filepath)
	if err != nil {
		log.Error("Error opening file:", err)
		return fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		log.Infof("Read line: %s", line)
		// Process the line as needed.
	}
	return nil
}
