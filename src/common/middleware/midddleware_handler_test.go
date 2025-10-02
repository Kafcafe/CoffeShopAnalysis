package middleware

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

var (
	rabbitContainer testcontainers.Container
	containerOnce   sync.Once
	containerHost   string
	containerPort   int
	containerErr    error
)

func setupRabbitContainer(t *testing.T) (host string, port int) {
	containerOnce.Do(func() {
		ctx := context.Background()

		rabbitContainer, containerErr = rabbitmq.Run(ctx,
			"rabbitmq:4.1.4-management",
			rabbitmq.WithAdminUsername("user"),
			rabbitmq.WithAdminPassword("password"),
			testcontainers.WithEnv(map[string]string{
				"RABBITMQ_DEFAULT_USER": "user",
				"RABBITMQ_DEFAULT_PASS": "password",
			}),
		)
		if containerErr != nil {
			return
		}

		containerHost, containerErr = rabbitContainer.Host(ctx)
		if containerErr != nil {
			return
		}

		mappedPort, containerErr := rabbitContainer.MappedPort(ctx, "5672")
		if containerErr != nil {
			return
		}
		containerPort = mappedPort.Int()
	})

	if containerErr != nil {
		t.Fatal(containerErr)
	}

	return containerHost, containerPort
}

func TestMain(m *testing.M) {
	// Setup is done in setupRabbitContainer via sync.Once
	code := m.Run()
	// Cleanup
	if rabbitContainer != nil {
		ctx := context.Background()
		rabbitContainer.Terminate(ctx)
	}
	os.Exit(code)
}

func createMiddleware(host string, port int) (*MiddlewareHandler, error) {
	// Test connection
	rabbitConf := NewRabbitConfig("user", "password", host, port)
	rabbitConn, err := NewRabbitConnection(&rabbitConf)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to RabbitMQ: %v", err)
	}

	middleHandler, err := NewMiddlewareHandler(rabbitConn)
	if err != nil {
		return nil, fmt.Errorf("Failed to create middleware channel: %v", err)
	}

	return middleHandler, nil
}

func TestRabbitConnection(t *testing.T) {
	host, port := setupRabbitContainer(t)
	middleHandler, err := createMiddleware(host, port)
	if err != nil {
		t.Fatalf("Failed to initialize middleware connection: %v", err)
	}

	defer middleHandler.Close()

	// Test declaring a queue
	queue, err := middleHandler.DeclareQueue("test_queue")
	if err != nil {
		t.Fatalf("Failed to declare queue: %v", err)
	}

	if queue.Name != "test_queue" {
		t.Errorf("Expected queue name 'test_queue', got '%s'", queue.Name)
	}
}

func TestDeclareExchange(t *testing.T) {
	host, port := setupRabbitContainer(t)
	middleHandler, err := createMiddleware(host, port)
	if err != nil {
		t.Fatalf("Failed to initialize middleware connection: %v", err)
	}

	defer middleHandler.Close()

	// Test declaring a direct exchange
	err = middleHandler.DeclareExchange("test_exchange", "direct")
	if err != nil {
		t.Fatalf("Failed to declare exchange: %v", err)
	}

	// Test declaring a topic exchange
	err = middleHandler.DeclareExchange("test_topic_exchange", "topic")
	if err != nil {
		t.Fatalf("Failed to declare topic exchange: %v", err)
	}
}

func TestBindQueue(t *testing.T) {
	host, port := setupRabbitContainer(t)
	middleHandler, err := createMiddleware(host, port)
	if err != nil {
		t.Fatalf("Failed to initialize middleware connection: %v", err)
	}

	defer middleHandler.Close()

	// Declare exchange and queue
	err = middleHandler.DeclareExchange("test_exchange", "direct")
	if err != nil {
		t.Fatalf("Failed to declare exchange: %v", err)
	}

	_, err = middleHandler.DeclareQueue("test_queue")
	if err != nil {
		t.Fatalf("Failed to declare queue: %v", err)
	}

	// Test binding queue to exchange
	err = middleHandler.BindQueue("test_queue", "test_exchange", "test_key")
	if err != nil {
		t.Fatalf("Failed to bind queue: %v", err)
	}
}

func TestWorkingQueueOneToOne(t *testing.T) {
	host, port := setupRabbitContainer(t)
	mh, err := createMiddleware(host, port)
	require.NoError(t, err)
	defer mh.Close()

	queue, err := mh.CreateQueue("queue_1to1")
	require.NoError(t, err)

	errChan := make(chan MessageMiddlewareError, 1)
	received := make(chan string, 1)

	queue.StartConsuming(func(msg amqp.Delivery) error {
		received <- string(msg.Body)
		_ = msg.Ack(false)
		return nil
	}, errChan)

	payload := []byte("hello-1to1")
	require.Equal(t, MessageMiddlewareSuccess, queue.Send(payload))

	select {
	case msg := <-received:
		require.Equal(t, "hello-1to1", msg)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestWorkingQueueOneToMany(t *testing.T) {
	host, port := setupRabbitContainer(t)
	mh, err := createMiddleware(host, port)
	require.NoError(t, err)
	defer mh.Close()

	queue, err := mh.CreateQueue("queue_1toN")
	require.NoError(t, err)

	errChan1 := make(chan MessageMiddlewareError, 1)
	errChan2 := make(chan MessageMiddlewareError, 1)
	received1 := make(chan string, 2)
	received2 := make(chan string, 2)

	queue.StartConsuming(func(msg amqp.Delivery) error {
		received1 <- string(msg.Body)
		_ = msg.Ack(false)
		return nil
	}, errChan1)

	queue.StartConsuming(func(msg amqp.Delivery) error {
		received2 <- string(msg.Body)
		_ = msg.Ack(false)
		return nil
	}, errChan2)

	for i := 0; i < 4; i++ {
		require.Equal(t, MessageMiddlewareSuccess, queue.Send([]byte("msg")))
	}

	total := 0
	timeout := time.After(5 * time.Second)
	for total < 4 {
		select {
		case <-received1:
			total++
		case <-received2:
			total++
		case <-timeout:
			t.Fatalf("timeout waiting for messages, got %d", total)
		}
	}
	require.Equal(t, 4, total)
}

func TestExchangeOneToOne(t *testing.T) {
	host, port := setupRabbitContainer(t)
	mh, err := createMiddleware(host, port)
	require.NoError(t, err)
	defer mh.Close()

	ex, err := mh.CreateDirectExchange("route_1to1")
	require.NoError(t, err)

	errChan := make(chan MessageMiddlewareError, 1)
	received := make(chan string, 1)

	ex.StartConsuming(func(msg amqp.Delivery) error {
		received <- string(msg.Body)
		_ = msg.Ack(false)
		return nil
	}, errChan)

	payload := []byte("exchange-1to1")
	require.Equal(t, MessageMiddlewareSuccess, ex.Send(payload))

	select {
	case msg := <-received:
		require.Equal(t, "exchange-1to1", msg)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestExchangeTopicOneToMany(t *testing.T) {
	host, port := setupRabbitContainer(t)
	mh, err := createMiddleware(host, port)
	require.NoError(t, err)
	defer mh.Close()

	// Declarar un exchange topic manualmente
	err = mh.DeclareExchange("test_topic_exchange", EXCHANGE_TYPE_TOPIC)
	require.NoError(t, err)

	// Declarar dos colas y bindearlas con la misma routing key
	qA, err := mh.DeclareQueue("queueTopicA")
	require.NoError(t, err)
	require.NoError(t, mh.BindQueue(qA.Name, "test_topic_exchange", "logs.info"))

	qB, err := mh.DeclareQueue("queueTopicB")
	require.NoError(t, err)
	require.NoError(t, mh.BindQueue(qB.Name, "test_topic_exchange", "logs.info"))

	// Crear interfaces para cada cola
	queueA := NewMessageMiddlewareQueue(qA.Name, mh.Channel, nil)
	queueB := NewMessageMiddlewareQueue(qB.Name, mh.Channel, nil)

	// Consumidores
	errChan1 := make(chan MessageMiddlewareError, 1)
	errChan2 := make(chan MessageMiddlewareError, 1)
	received1 := make(chan string, 1)
	received2 := make(chan string, 1)

	queueA.StartConsuming(func(msg amqp.Delivery) error {
		received1 <- string(msg.Body)
		_ = msg.Ack(false)
		return nil
	}, errChan1)

	queueB.StartConsuming(func(msg amqp.Delivery) error {
		received2 <- string(msg.Body)
		_ = msg.Ack(false)
		return nil
	}, errChan2)

	// Enviar usando Publish directo
	payload := []byte("topic-broadcast")
	err = mh.Channel.Publish(
		"test_topic_exchange",
		"logs.info",
		false,
		false,
		amqp.Publishing{ContentType: "text/plain", Body: payload},
	)
	require.NoError(t, err)

	// Validar que ambos reciban
	got1, got2 := false, false
	timeout := time.After(5 * time.Second)
	for !(got1 && got2) {
		select {
		case msg := <-received1:
			require.Equal(t, "topic-broadcast", msg)
			got1 = true
		case msg := <-received2:
			require.Equal(t, "topic-broadcast", msg)
			got2 = true
		case <-timeout:
			t.Fatal("timeout waiting for both consumers to receive message")
		}
	}
}
