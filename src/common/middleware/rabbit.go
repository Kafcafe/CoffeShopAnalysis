package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type Rabbit struct {
	Conn    *amqp.Connection
	Channel MiddlewareChannel
}

func NewRabbit(user, password, host string) (*Rabbit, error) {
	conn, err := amqp.Dial("amqp://" + user + ":" + password + "@" + host + ":5672/")
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return &Rabbit{
		Conn:    conn,
		Channel: ch,
	}, nil
}

func (r *Rabbit) Close() error {
	if err := r.Channel.Close(); err != nil {
		return err
	}
	if err := r.Conn.Close(); err != nil {
		return err
	}
	return nil
}

func (r *Rabbit) DeclareQueue(queueName string) (*amqp.Queue, error) {
	q, err := r.Channel.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)

	return &q, err
}

func (r *Rabbit) DeclareExchange(exchangeName string, exchangeType string) error {
	err := r.Channel.ExchangeDeclare(
		exchangeName, // name
		exchangeType, // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	return err
}

func (r *Rabbit) BindQueue(queueName, exchangeName, routingKey string) error {
	err := r.Channel.QueueBind(
		queueName,    // queue name
		routingKey,   // routing key
		exchangeName, // exchange
		false,
		nil,
	)
	return err
}

func (r *Rabbit) ConsumeQueue(queueName string) (<-chan amqp.Delivery, error) {
	msgs, err := r.Channel.Consume(
		queueName, // queue
		"",        // consumer
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	return msgs, err
}
