package middleware

import (
	"context"
	"os"
	"sync"
	"testing"

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

func TestRabbitConnection(t *testing.T) {
	host, port := setupRabbitContainer(t)

	// Test connection
	rabbit, err := NewRabbit("user", "password", host, port)
	if err != nil {
		t.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer rabbit.Close()

	// Test declaring a queue
	queue, err := rabbit.DeclareQueue("test_queue")
	if err != nil {
		t.Fatalf("Failed to declare queue: %v", err)
	}

	if queue.Name != "test_queue" {
		t.Errorf("Expected queue name 'test_queue', got '%s'", queue.Name)
	}
}

func TestDeclareExchange(t *testing.T) {
	host, port := setupRabbitContainer(t)

	rabbit, err := NewRabbit("user", "password", host, port)
	if err != nil {
		t.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer rabbit.Close()

	// Test declaring a direct exchange
	err = rabbit.DeclareExchange("test_exchange", "direct")
	if err != nil {
		t.Fatalf("Failed to declare exchange: %v", err)
	}

	// Test declaring a topic exchange
	err = rabbit.DeclareExchange("test_topic_exchange", "topic")
	if err != nil {
		t.Fatalf("Failed to declare topic exchange: %v", err)
	}
}

func TestBindQueue(t *testing.T) {
	host, port := setupRabbitContainer(t)

	rabbit, err := NewRabbit("user", "password", host, port)
	if err != nil {
		t.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer rabbit.Close()

	// Declare exchange and queue
	err = rabbit.DeclareExchange("test_exchange", "direct")
	if err != nil {
		t.Fatalf("Failed to declare exchange: %v", err)
	}

	_, err = rabbit.DeclareQueue("test_queue")
	if err != nil {
		t.Fatalf("Failed to declare queue: %v", err)
	}

	// Test binding queue to exchange
	err = rabbit.BindQueue("test_queue", "test_exchange", "test_key")
	if err != nil {
		t.Fatalf("Failed to bind queue: %v", err)
	}
}
