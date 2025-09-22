
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