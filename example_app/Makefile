IMAGE_NAME ?= waymark-example-app
COMPOSE_CMD ?= docker compose -f docker-compose.yml
DOCKER_TEST_TIMEOUT ?= 300
DOCKER_LOG_TAIL ?= 200

.PHONY: build up down logs docker-test

build:
	@echo "Building $(IMAGE_NAME) image"
	docker build -f Dockerfile -t $(IMAGE_NAME) ..

up:
	$(COMPOSE_CMD) up --build -d

logs:
	$(COMPOSE_CMD) logs -f

down:
	$(COMPOSE_CMD) down -v

docker-test:
	@echo "Running tests in docker-compose environment (timeout $(DOCKER_TEST_TIMEOUT)s)"
	$(COMPOSE_CMD) up -d postgres daemons
	@echo "Waiting for postgres to be healthy..."
	@until $(COMPOSE_CMD) exec -T postgres pg_isready -U waymark >/dev/null 2>&1; do sleep 1; done
	@echo "Waiting for worker bridge port 24118 to be ready..."
	@until $(COMPOSE_CMD) exec -T daemons python -c "import socket; s = socket.create_connection(('127.0.0.1', 24118), 1); s.close()" >/dev/null 2>&1; do sleep 1; done
	@TEST_EXIT_CODE=0; \
	timeout $(DOCKER_TEST_TIMEOUT) $(COMPOSE_CMD) run -T --rm -e WAYMARK_DATABASE_URL=postgresql://waymark:waymark@postgres:5432/waymark_example webapp pytest -vvv </dev/null || TEST_EXIT_CODE=$$?; \
	echo ""; \
	echo "=== Daemon Logs ==="; \
	$(COMPOSE_CMD) logs --tail=$(DOCKER_LOG_TAIL) daemons || true; \
	echo ""; \
	echo "=== Webapp Logs ==="; \
	$(COMPOSE_CMD) logs --tail=$(DOCKER_LOG_TAIL) webapp || true; \
	echo ""; \
	$(COMPOSE_CMD) down -v || true; \
	exit $$TEST_EXIT_CODE
