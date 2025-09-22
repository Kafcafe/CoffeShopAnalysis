package client

type ClientConfig struct {
	Id             string
	serverAddress  string
	batchMaxAmount string
}

func NewClientConfig(id, serverAddress, batchMaxAmount string) *ClientConfig {
	return &ClientConfig{
		Id:             id,
		serverAddress:  serverAddress,
		batchMaxAmount: batchMaxAmount,
	}
}
