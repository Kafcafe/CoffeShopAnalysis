package clientHandler

type ServerConfig struct {
	IP   string
	Port int
}

func NewServerConfig(ip string, port int) *ServerConfig {
	return &ServerConfig{
		IP:   ip,
		Port: port,
	}
}
