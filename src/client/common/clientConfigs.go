package client

type ClientConfig struct {
	serverAddress  string
	batchMaxAmount int
	dataPath       string
}

func NewClientConfig(serverAddress, dataPath string, batchMaxAmount int) *ClientConfig {
	return &ClientConfig{
		serverAddress:  serverAddress,
		batchMaxAmount: batchMaxAmount,
		dataPath:       dataPath,
	}
}
