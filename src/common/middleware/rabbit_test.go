package middleware

import (
	"context"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestRabbitConnection(t *testing.T) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "rabbitmq:4.1.4-management",
		ExposedPorts: []string{"5672/tcp"},
		Env: map[string]string{
			"RABBITMQ_DEFAULT_USER": "user",
			"RABBITMQ_DEFAULT_PASS": "password",
		},
		WaitingFor: wait.ForLog("Server startup complete"),
	}

	rabbitContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rabbitContainer.Terminate(ctx)

	host, err := rabbitContainer.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}

	port, err := rabbitContainer.MappedPort(ctx, "5672")
	if err != nil {
		t.Fatal(err)
	}

	// Test connection
	rabbit, err := NewRabbit("user", "password", host, port.Int())
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
