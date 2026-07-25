IMAGE_NAME ?= waymark-example-javascript
COMPOSE_CMD ?= docker compose -f docker-compose.yml

.PHONY: build up down logs test

build:
	docker build -f Dockerfile -t $(IMAGE_NAME) ../..

up:
	$(COMPOSE_CMD) up --build

down:
	$(COMPOSE_CMD) down -v

logs:
	$(COMPOSE_CMD) logs -f

test:
	$(COMPOSE_CMD) run --build --rm webapp npm test
