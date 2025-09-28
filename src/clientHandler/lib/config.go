package clientHandler

type ClientHandlerConfig struct {
	Ip   string
	Port int
}

// NewClientHandlerConfig creates a new ClientHandlerConfig instance.
// Parameters:
//
//	ip: the server IP address
//	port: the server port number
//
// Returns a pointer to the ClientHandlerConfig.
func NewClientHandlerConfig(ip string, port int) *ClientHandlerConfig {
	return &ClientHandlerConfig{
		Ip:   ip,
		Port: port,
	}
}
