# ==============================================================================
# CoffeeShopAnalysis - Distributed System Build Configuration
# ==============================================================================
# This Makefile provides convenient commands for building, running, and managing
# the distributed coffee shop data analysis system.
#
# Quick Start:
#   make deps     - Install dependencies
#   make image    - Build Docker images
#   make up       - Start all services
#   make down     - Stop all services
#   make test     - Run tests (containerized, no Go required)
#   make clean    - Clean up Docker resources
# ==============================================================================

SHELL := /bin/bash
PWD := $(shell pwd)

# Default target - builds Docker images
default: build

# Empty target for future extensions
all:

# ==============================================================================
# DEPENDENCIES & SETUP
# ==============================================================================

# Install and organize Go module dependencies
# Runs go mod tidy to clean up dependencies and go mod vendor to create vendor directory
deps:
	go mod tidy
	go mod vendor

# ==============================================================================
# DOCKER IMAGE BUILDING
# ==============================================================================

# Build Docker images for server and client components
# Creates images with 'latest' tag for both services
# Note: Client build generates intermediate stages that can be cleaned up periodically
#       (see commented command below for cleanup, but leave commented for faster rebuilds)

# Execute this command from time to time to clean up intermediate stages generated
# during client build (your hard drive will like this :) ). Don't left uncommented if you
# want to avoid rebuilding client image every time the docker-compose-up command
# is executed, even when client code has not changed
# docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`
image:
	docker build -f ./server/Dockerfile -t "server:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .

	# docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`
.PHONY: image

# ==============================================================================
# DOCKER COMPOSE MANAGEMENT
# ==============================================================================
# Default compose file: docker-compose-dev.yaml (development environment)
# Use FILE=filename to specify alternative compose file
# Example: FILE=docker-compose.yaml make up
# ==============================================================================

FILE ?= docker-compose-dev.yaml

# Start all services with build
# Uses specified compose file (default: docker-compose-dev.yaml)
# Runs in detached mode with --build flag to ensure latest images
up:
	docker compose -f $(FILE) up -d --build
.PHONY: up

# Stop and remove all services and networks
# Gracefully stops containers with 1s timeout, then removes them
down:
	docker compose -f $(FILE) stop -t 1
	docker compose -f $(FILE) down
.PHONY: down

# View real-time logs from services
# Use positional argument to specify which services to log:
#   make logs              - All services (default)
#   make logs no-rabbit  - All services except rabbitmq
#   make logs only-rabbit     - Only rabbitmq service
# Follows log output continuously until interrupted (Ctrl+C)
logs:
	@filter=$$(echo $(MAKECMDGOALS) | sed 's/logs //'); \
	if [ "$$filter" = "only-rabbit" ]; then \
		docker compose -f $(FILE) logs -f rabbitmq; \
	elif [ "$$filter" = "no-rabbit" ]; then \
		docker compose -f $(FILE) logs -f $$(docker compose -f $(FILE) config --services | grep -v rabbitmq); \
	else \
		docker compose -f $(FILE) logs -f; \
	fi
.PHONY: logs only-rabbit no-rabbit

# Start previously stopped services
# Resumes containers without rebuilding
start:
	docker compose -f $(FILE) start
.PHONY: start

# Stop all running services without removing them
# Containers remain available for quick restart with 'make start'
stop:
	docker compose -f $(FILE) stop
.PHONY: stop


test test-v:
	@echo "🧪 Running tests for common/middleware using Docker"
	@echo ""; \
	verbosity=""; \
	if [ "$@" = "test-v" ]; then \
		verbosity="-v"; \
	fi; \
	docker build -f ./src/common/Dockerfile.test -t go-test:latest ./src/common/ && \
	docker run --rm -v /var/run/docker.sock:/var/run/docker.sock go-test:latest test ./middleware $$verbosity -coverpkg=common/middleware
.PHONY: test test-v

raw-test raw-test-v:
	@echo "🧪 Running tests for common/middleware"
	@echo ""; \
	verbosity=""; \
	if [ "$@" = "raw-test-v" ]; then \
		verbosity="-v"; \
	fi; \
	cd src/common/middleware && go test $$verbosity -coverpkg=common/middleware
	cd src/group/yearmonth && go test $$verbosity -coverpkg=group/yearmonth
.PHONY: raw-test raw-test-v

# ==============================================================================
# DOCKER CLEANUP COMMANDS
# ==============================================================================
# Commands for cleaning up Docker resources and freeing disk space
# Use with caution, especially clean-all-images and clean-system
# ==============================================================================

# Remove unused Docker images only (safe cleanup)
clean-images:
	docker image prune -a
.PHONY: clean-images

# Remove ALL Docker images (use with extreme caution!)
# This will remove every Docker image on your system
# The '|| true' prevents errors if no images exist
clean-all-images:
	docker rmi $$(docker images -q) 2>/dev/null || true
.PHONY: clean-all-images

# Complete system cleanup (nuclear option!)
# Removes: containers, images, volumes, networks, and build cache
# This is the most aggressive cleanup - use only when necessary
clean-system:
	docker system prune -a --volumes
.PHONY: clean-system

# Remove all stopped containers
# Safe way to clean up container resources without affecting images
clean-containers:
	docker container prune
.PHONY: clean-containers

# Basic cleanup: removes stopped containers and unused images
clean: clean-containers clean-images
	@echo "Limpieza básica completada"
.PHONY: clean

