package middleware

type RabbitConfig struct {
	User     string
	Password string
	Host     string
	Port     int
}

func NewRabbitConfig(user, password, host string, port int) RabbitConfig {
	return RabbitConfig{
		User:     user,
		Password: password,
		Host:     host,
		Port:     port,
	}
}
