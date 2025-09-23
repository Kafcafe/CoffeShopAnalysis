package client

type ClientConfig struct {
	Id             string
	serverAddress  string
	batchMaxAmount int
	dataPath       string
}

func NewClientConfig(id, serverAddress, dataPath string, batchMaxAmount int) *ClientConfig {
	return &ClientConfig{
		Id:             id,
		serverAddress:  serverAddress,
		batchMaxAmount: batchMaxAmount,
		dataPath:       dataPath,
	}
}
