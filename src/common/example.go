package main

import (
	"os"
	"time"

	middleware "common/middleware"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("yearmonth")

func InitLogger(logLevel string) error {
	baseBackend := logging.NewLogBackend(os.Stdout, "", 0)
	format := logging.MustStringFormatter(
		`%{time:2006-01-02 15:04:05.000} %{level:.5s} %{message}`,
	)
	backendFormatter := logging.NewBackendFormatter(baseBackend, format)

	backendLeveled := logging.AddModuleLevel(backendFormatter)
	logLevelCode, err := logging.LogLevel(logLevel)
	if err != nil {
		return err
	}
	backendLeveled.SetLevel(logLevelCode, "")

	// Set the backends to be used.
	logging.SetBackend(backendLeveled)
	return nil
}

func failOnError(err error) {
	if err != nil {
		log.Fatalf("Failed: %s", err)
	}
}

func processMessages(consumeChannel middleware.ConsumeChannel, done chan error) {
	for msg := range *consumeChannel {
		log.Infof("Received a message: %s", msg.Body)
		// Simula procesamiento
		time.Sleep(1 * time.Second)
		log.Infof("Processed message: %s", msg.Body)
		msg.Ack(false)
	}
	done <- nil
}

func queueExample(rabbit *middleware.Rabbit) {
	queueName := "year_month_queue"

	log.Infof("Queue: %s", queueName)

	q, err := rabbit.DeclareQueue(queueName)
	failOnError(err)

	msgs, err := rabbit.ConsumeQueue(q.Name)
	failOnError(err)

	forever := make(chan bool)

	queue := middleware.NewMessageMiddlewareQueue(q.Name, rabbit.Channel, &msgs)

	go func() {
		queue.StartConsuming(processMessages)
	}()

	log.Info("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func exchangeExample(rabbit *middleware.Rabbit) {
	exchangeName := "year_month_exchange"
	queueName := ""
	routingKey := "year_month"

	log.Infof("Exchange: %s, Queue: %s, Routing Key: %s", exchangeName, queueName, routingKey)

	err := rabbit.DeclareExchange(exchangeName, "direct")
	failOnError(err)

	q, err := rabbit.DeclareQueue(queueName)
	failOnError(err)

	err = rabbit.BindQueue(q.Name, exchangeName, routingKey)
	failOnError(err)

	msgs, err := rabbit.ConsumeQueue(q.Name)
	failOnError(err)

	exchange := middleware.NewMessageMiddlewareExchange(exchangeName, []string{routingKey}, rabbit.Channel, &msgs)

	go func() {
		exchange.StartConsuming(processMessages)
	}()

	go func() {
		time.Sleep(3 * time.Second)
		exchangeSendExample(rabbit)
	}()

	log.Info("Waiting for messages for 10 seconds. To exit press CTRL+C")
	time.Sleep(10 * time.Second)
	log.Info("Stopping now.")
	exchange.StopConsuming()
}

func exchangeSendExample(rabbit *middleware.Rabbit) {
	exchangeName := "year_month_exchange"
	routingKey := "year_month"

	log.Infof("Exchange: %s, Routing Key: %s", exchangeName, routingKey)

	err := rabbit.DeclareExchange(exchangeName, "direct")
	failOnError(err)

	exchange := middleware.NewMessageMiddlewareExchange(exchangeName, []string{routingKey}, rabbit.Channel, nil)

	for i := range 5 {
		message := []byte("Message number " + string(rune(i+'0')))
		err := exchange.Send(message)
		if err != 0 {
			log.Errorf("Failed to send message: %v", err)
		} else {
			log.Infof("Sent message: %s", message)
		}
	}
}

func main() {
	InitLogger("DEBUG")

	rabbit, err := middleware.NewRabbit("guest", "guest", "localhost", 5672)
	failOnError(err)
	defer rabbit.Close()

	queueExample(rabbit)
	//exchangeExample(rabbit)
}
