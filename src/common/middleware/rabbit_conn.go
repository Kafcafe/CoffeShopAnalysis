package middleware

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	AMQP_PROTOCOL = "amqp"
)

type RabbitConnection struct {
	conn *amqp.Connection
}

func NewRabbitConnection(conf *RabbitConfig) (*RabbitConnection, error) {
	connectionString := fmt.Sprintf("%s://%s:%s@%s:%d/", AMQP_PROTOCOL, conf.User, conf.Password, conf.Host, conf.Port)
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		return nil, err
	}

	return &RabbitConnection{
		conn: conn,
	}, nil
}

func (rc *RabbitConnection) CreateNewChannel() (MiddlewareChannel, error) {
	ch, err := rc.conn.Channel()
	if err != nil {
		return nil, err
	}

	return ch, nil
}

func (rc *RabbitConnection) Close() error {
	if rc.conn.IsClosed() {
		return nil
	}
	if err := rc.conn.Close(); err != nil {
		return err
	}

	return nil
}
