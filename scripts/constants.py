
"""
Docker Compose YAML templates for distributed coffee shop analysis system.

This module contains string templates used to generate Docker Compose files
for a distributed system that analyzes coffee shop data. The templates define:

- Network configuration for service communication
- RabbitMQ message broker service with management interface
- Client handler service for coordinating clients
- Client service template for data processing clients

All templates use YAML format and include proper service dependencies,
health checks, and network configuration for the distributed system.
"""

# Network configuration template for Docker Compose
# Defines a custom network with specific subnet for service communication
NETWORK_TEMPLATE = """
name: tp1
networks:
  analysis_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24

"""

# RabbitMQ message broker service template
# Provides message queue functionality with management interface
# Includes health check to ensure service readiness before starting dependent services
RABBITMQ_SERVICE_TEMPLATE = """
services:
    rabbitmq:
        image: "rabbitmq:management"
        container_name: "rabbitmq"
        hostname: "rabbitmq"
        ports:
          - "5672:5672"
          - "15672:15672"
        networks:
          - analysis_net
        environment:
          RABBITMQ_DEFAULT_USER: user
          RABBITMQ_DEFAULT_PASS: password
        healthcheck:
          test: ["CMD", "rabbitmq-diagnostics", "ping"]
          interval: 10s
          timeout: 5s
          retries: 3
          start_period: 30s
"""

# Client service template (parameterized by client ID)
# Each client processes different types of coffee shop data files
# Template is formatted with unique client ID when generating compose file
CLIENTS_TEMPLATE = """
    client{id}:
        container_name: "client{id}"
        entrypoint: /client
        environment:
          CLIENT_ID: "{id}"
          FILETYPES: "transactions,transaction_items,stores,menu,users"
        depends_on:
          - client_handler
        networks:
          - analysis_net
        build:
          context: ./src/client
          dockerfile: Dockerfile
        volumes:
          - ./src/client/config.yaml:/config.yaml
          - ./.data:/data
"""

# Client handler service template
# Coordinates client activities and manages the distributed processing workflow
# Waits for RabbitMQ to be healthy before starting
CLIENT_HANDLER_TEMPLATE = """
    client_handler:
        container_name: "clientHandler"
        entrypoint: /client
        depends_on:
          rabbitmq:
            condition: service_healthy
        networks:
          - analysis_net
        hostname: "server"
        build:
          context: ./src/clientHandler
          dockerfile: Dockerfile
        volumes:
          - ./src/clientHandler/config.yaml:/server.yaml 
"""
