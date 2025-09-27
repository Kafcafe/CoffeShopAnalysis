# CoffeeShopAnalysis

Distributed coffee shop data analysis system using microservices architecture with Docker, RabbitMQ, and Go.

## Table of Contents

1. [Quick Start](#quick-start)
1. [Makefile Commands](#makefile-commands)
   1. [Dependencies & Setup](#dependencies--setup)
   1. [Building Docker Images](#building-docker-images)
   1. [Docker Compose Management](#docker-compose-management)
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
- `make logs` - View real-time logs from all services
  - Follows log output continuously
- `make start` - Start previously stopped services
- `make stop` - Stop all running services without removing them

**Note**: Use `FILE=docker-compose.yaml make up` to use alternative compose file

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
