package client

type ClientConfig struct {
	Id             string
	serverAddress  string
	batchMaxAmount string
	dataPath       string
}

func NewClientConfig(id, serverAddress, batchMaxAmount, dataPath string) *ClientConfig {
	return &ClientConfig{
		Id:             id,
		serverAddress:  serverAddress,
		batchMaxAmount: batchMaxAmount,
		dataPath:       dataPath,
	}
}
