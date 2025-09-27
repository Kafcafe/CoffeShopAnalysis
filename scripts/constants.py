
NETWORK_TEMPLATE = """
name: tp1
networks:
  analysis_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24

"""

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

CLIENT_HANDLER_TEMPLATE = """
    clientHandler:
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