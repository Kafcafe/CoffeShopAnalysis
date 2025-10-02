
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
name: coffee-shop-analysis-system
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
        image: "rabbitmq:4.1.4-management"
        container_name: "rabbitmq"
        hostname: "rabbitmq"
        ports:
          - "5672:5672"
          - "15672:15672"
        networks:
          - analysis_net
        environment:
          RABBITMQ_DEFAULT_USER: user
          RABBITMQ_DEFAULT_PASS: user
        healthcheck:
          test: ["CMD", "rabbitmq-diagnostics", "ping"]
          interval: 10s
          timeout: 5s
          retries: 3
          start_period: 30s
"""


# Client handler service template
# Coordinates client activities and manages the distributed processing workflow
# Waits for RabbitMQ to be healthy before starting
CLIENT_HANDLER_TEMPLATE = """
    client-handler:
        container_name: "client-handler"
        depends_on:
          rabbitmq:
            condition: service_healthy
        networks:
          - analysis_net
        environment:
          RABBITMQ_HOST: rabbitmq
          RABBITMQ_PORT: 5672
          RABBITMQ_USER: user
          RABBITMQ_PASS: user
        hostname: "server"
        build:
          context: ./src/
          dockerfile: clientHandler/Dockerfile
        volumes:
          - ./src/clientHandler/config.yaml:/config.yaml 
"""

FILTER_TEMPLATE = """
    filter{id}:
        container_name: "filter{id}"
        depends_on:
          rabbitmq:
            condition: service_healthy
        networks:
          - analysis_net
        environment:
          RABBITMQ_HOST: rabbitmq
          RABBITMQ_PORT: 5672
          RABBITMQ_USER: user
          RABBITMQ_PASS: user
          FILTER_TYPE: {filter_type}
          FILTER_ID: {id}
          FILTER_COUNT: {filter_count}
        build:
          context: ./src/
          dockerfile: filters/Dockerfile
        volumes:
          - ./src/filters/config.yaml:/config.yaml 
"""

# Client service template (parameterized by client ID)
# Each client processes different types of coffee shop data files
# Template is formatted with unique client ID when generating compose file
CLIENTS_TEMPLATE = """
    client{id}:
        container_name: "client{id}"
        environment:
          CLIENT_ID: "{id}"
          FILETYPES: "transactions,transaction_items,stores,menu,users"
        depends_on:
          - client-handler
        networks:
          - analysis_net
        build:
          context: ./src/client
          dockerfile: Dockerfile
        volumes:
          - ./src/client/config.yaml:/config.yaml
          - ./.data:/data
"""

GROUP_TEMPLATE = """
    group{id}:
        container_name: "group{id}"
        depends_on:
          rabbitmq:
            condition: service_healthy
        networks:
          - analysis_net
        environment:
          RABBITMQ_HOST: rabbitmq
          RABBITMQ_PORT: 5672
          RABBITMQ_USER: user
          RABBITMQ_PASS: user
          GROUP_TYPE: {group_type}
          GROUP_ID: {id}
          GROUP_COUNT: {group_count}
        build:
          context: ./src/
          dockerfile: group/Dockerfile
        volumes:
          - ./src/group/config.yaml:/config.yaml 
"""

JOIN_TEMPLATE = """
    join{id}:
        container_name: "join{id}"
        depends_on:
          rabbitmq:
            condition: service_healthy
        networks:
          - analysis_net
        environment:
          RABBITMQ_HOST: rabbitmq
          RABBITMQ_PORT: 5672
          RABBITMQ_USER: user
          RABBITMQ_PASS: user
          JOIN_TYPE: {join_type}
          JOIN_ID: {id}
          JOIN_COUNT: {join_count}
        build:
          context: ./src/
          dockerfile: join/Dockerfile
        volumes:
          - ./src/join/config.yaml:/config.yaml 
"""


TOP_K_TEMPLATE = """
    topk{id}:
        container_name: "topk{id}"
        depends_on:
          rabbitmq:
            condition: service_healthy
        networks:
          - analysis_net
        environment:
          RABBITMQ_HOST: rabbitmq
          RABBITMQ_PORT: 5672
          RABBITMQ_USER: user
          RABBITMQ_PASS: user
        build:
          context: ./src/
          dockerfile: topk/Dockerfile
        volumes:
          - ./src/topk/config.yaml:/config.yaml
"""