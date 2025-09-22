package client

import (
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
	return nil
}
