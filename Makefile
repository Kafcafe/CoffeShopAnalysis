SHELL := /bin/bash
PWD := $(shell pwd)

default: build

all:

deps:
	go mod tidy
	go mod vendor

//To do: check this command
image:
	docker build -f ./server/Dockerfile -t "server:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
	# Execute this command from time to time to clean up intermediate stages generated 
	# during client build (your hard drive will like this :) ). Don't left uncommented if you 
	# want to avoid rebuilding client image every time the docker-compose-up command 
	# is executed, even when client code has not changed
	# docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`
.PHONY: image

FILE ?= docker-compose-dev.yaml

up: 
	docker compose -f $(FILE) up -d --build
.PHONY: up

down:
	docker compose -f $(FILE) stop -t 1
	docker compose -f $(FILE) down
.PHONY: down

logs:
	docker compose -f $(FILE) logs -f
.PHONY: logs

start:
	docker compose -f $(FILE) start
.PHONY: start

stop:
	docker compose -f $(FILE) stop
.PHONY: stop