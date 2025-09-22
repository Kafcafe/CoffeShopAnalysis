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

up: 
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: up

down:
	docker compose -f docker-compose-dev.yaml stop -t 1
	docker compose -f docker-compose-dev.yaml down
.PHONY: down

logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: logs

start:
	docker compose -f docker-compose-dev.yaml start
.PHONY: start

stop:
	docker compose -f docker-compose-dev.yaml stop
.PHONY: stop

uptest:
	docker compose -f docker-compose.yaml up -d --build
.PHONY: uptest

downtest:
	docker compose -f docker-compose.yaml stop -t 1
	docker compose -f docker-compose.yaml down
.PHONY: downtest

logstest:
	docker compose -f docker-compose.yaml logs -f
.PHONY: logstest

starttest:
	docker compose -f docker-compose.yaml start
.PHONY: starttest

stoptest:
	docker compose -f docker-compose.yaml stop -t 1
	docker compose -f docker-compose.yaml down
.PHONY: stoptest