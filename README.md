# Coffee Shop Analysis Distributed System

Distributed coffee shop data analysis system using Docker, RabbitMQ, and Go.

<p align="center">
    <img src="./docs/logo.png" alt="Kafcafe logo" height="400px">
</p>

### Team members

| Name                          | Padrón | Email              |
| ----------------------------- | ------ | ------------------ |
| Castro Martinez, Jose Ignacio | 106957 | jcastrom@fi.uba.ar |
| Diem, Walter Gabriel          | 105618 | wdiem@fi.uba.ar    |
| Gestoso, Ramiro               | 105950 | rgestoso@fi.uba.ar |

## Table of Contents

1. [Quick Start](#quick-start)
1. [Makefile Commands](#makefile-commands)
    1. [Dependencies & Setup](#dependencies--setup)
    1. [Building Docker Images](#building-docker-images)
    1. [Docker Compose Management](#docker-compose-management)
    1. [Testing](#testing)
    1. [Cleanup Commands](#cleanup-commands)
    1. [Default Targets](#default-targets)

- `docker-compose-dev.yaml` -> Use this for development
- `docker-compose.yaml` -> Temporary testing file

## Makefile Commands

### Dependencies & Setup

- `make deps` - Install and organize Go module dependencies
  - Runs `go mod tidy` and `go mod vendor`

### Building Docker Images

- `make image` - Build Docker images for server and client components
  - Builds both server and client images with latest tag
  - Includes cleanup command for intermediate build stages (commented out for faster rebuilds)

### Docker Compose Management

- `make up` - Start all services with rebuild
  - Uses `docker-compose-dev.yaml` by default (or specify with `FILE=filename`)
  - Runs containers in detached mode with `--build` flag
- `make down` - Stop and remove all services
  - Gracefully stops containers with 1s timeout, then removes them
- `make logs` - View real-time logs from services
  - `make logs` - All services (default)
  - `make logs no-rabbitmq` - All services except rabbitmq
  - `make logs rabbitmq` - Only rabbitmq service
  - Follows log output continuously
- `make start` - Start previously stopped services
- `make stop` - Stop all running services without removing them

**Note**: Use `FILE=docker-compose.yaml make up` to use alternative compose file

### Testing

- `make test` - Run tests for common/middleware using Docker (no Go installation required)
  - Runs containerized tests with testcontainers support
  - Includes coverage reporting
- `make test-v` - Run tests with verbose output
  - Same as `make test` but with detailed test output
- `make raw-test` - Run tests directly with Go (requires Go installation)
- `make raw-test-v` - Run tests directly with Go and verbose output

**Note**: Containerized tests (`make test`) run in a Docker container and require Docker socket access for testcontainers. Raw tests (`make raw-test`) require Go to be installed locally.

### Cleanup Commands

- `make clean` - Basic cleanup (containers + unused images)
- `make clean-containers` - Remove all stopped containers
- `make clean-images` - Remove unused Docker images only
- `make clean-all-images` - Remove ALL Docker images (use with caution)
- `make clean-system` - Complete system cleanup including volumes
  - Removes everything: containers, images, volumes, networks

### Default Targets

- `make` or `make default` - Same as `make image` (build Docker images)
- `make all` - Currently empty target
