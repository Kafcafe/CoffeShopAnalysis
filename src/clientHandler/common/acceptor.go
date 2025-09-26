package clientHandler

type Acceptor struct {
	serverConfigs *ServerConfig
}

func NewAcceptor(serverConfigs *ServerConfig) *Acceptor {

	return &Acceptor{
		serverConfigs: serverConfigs,
	}
}

func (a *Acceptor) Run() error {
	// Implement the logic to accept client connections and handle them
	return nil
}
