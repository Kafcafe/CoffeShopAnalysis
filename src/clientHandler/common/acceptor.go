package clientHandler

import (
	"fmt"
	"net"

	"github.com/op/go-logging"
)

const transport = "tcp"

var log = logging.MustGetLogger("log")

type Acceptor struct {
	listener  net.Listener
	isRunning bool
}

func NewAcceptor(serverConfigs *ServerConfig) (*Acceptor, error) {

	listenAddr := fmt.Sprintf("%s:%d", serverConfigs.IP, serverConfigs.Port)
	listener, err := net.Listen(transport, listenAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to start server: %v", err)
	}
	return &Acceptor{
		listener: listener,
	}, nil
}

func (a *Acceptor) Run() error {
	// Implement the logic to accept client connections and handle them
	for a.isRunning {
		conn, err := a.listener.Accept()
		if err != nil {
			log.Errorf("Failed to accept connection: %v", err)
			return err
		}

		clientHandler := NewClientHandler(conn)
		err = clientHandler.Handle()

		if err != nil {
			log.Errorf("Error handling client connection: %v", err)
		}

		clientHandler.Shutdown()

	}

	return nil
}
