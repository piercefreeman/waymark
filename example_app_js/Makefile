IMAGE_NAME ?= rappel-example-app-js
COMPOSE_CMD ?= docker compose -f docker-compose.yml

.PHONY: build up down logs

build:
	@echo "Building $(IMAGE_NAME) image"
	docker build -f Dockerfile -t $(IMAGE_NAME) ..

up:
	$(COMPOSE_CMD) up --build -d

logs:
	$(COMPOSE_CMD) logs -f

down:
	$(COMPOSE_CMD) down -v
