package clientHandler

type AcceptorConfig struct {
	Ip             string
	Port           int
	rabbitUser     string
	rabbitPassword string
	rabbitHost     string
	rabbitPort     int
}

// NewClientHandlerConfig creates a new ClientHandlerConfig instance.
// Parameters:
//
//	ip: the server IP address
//	port: the server port number
//
// Returns a pointer to the ClientHandlerConfig.
func NewAcceptorConfig(ip string, port int, rabbitUser, rabbitPassword, rabbitHost string, rabbitPort int) *AcceptorConfig {
	return &AcceptorConfig{
		Ip:             ip,
		Port:           port,
		rabbitUser:     rabbitUser,
		rabbitPassword: rabbitPassword,
		rabbitHost:     rabbitHost,
		rabbitPort:     rabbitPort,
	}
}
