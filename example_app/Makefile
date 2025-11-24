IMAGE_NAME ?= rappel-example-app
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
	@timeout 30 sh -c 'until docker compose exec postgres pg_isready -U rappel > /dev/null 2>&1; do sleep 1; done' || (echo "Postgres failed to become healthy" && exit 1)
	@echo "Waiting for workers to be ready..."
	@sleep 3
	@echo "Checking that daemons are running and ready..."
	@timeout 60 sh -c 'until $(COMPOSE_CMD) ps --status=running --services | grep -q "^daemons$$"; do sleep 1; done' || (echo "Daemons failed to stay running" && $(COMPOSE_CMD) logs daemons || true; $(COMPOSE_CMD) down -v; exit 1)
	@timeout 60 sh -c 'until $(COMPOSE_CMD) logs --tail=50 daemons 2>&1 | grep -q "python worker pool started"; do sleep 1; done' || (echo "Daemons never reported ready" && $(COMPOSE_CMD) logs daemons || true; $(COMPOSE_CMD) down -v; exit 1)
	@set -e; \
	TEST_EXIT_CODE=0; \
	timeout $(DOCKER_TEST_TIMEOUT) $(COMPOSE_CMD) run --rm -e DATABASE_URL=postgresql://rappel:rappel@postgres:5432/rappel_example webapp uv run --active pytest -vvv || TEST_EXIT_CODE=$$?; \
	echo ""; \
	echo "=== Daemon Logs ==="; \
	$(COMPOSE_CMD) logs --tail=$(DOCKER_LOG_TAIL) daemons || true; \
	echo ""; \
	echo "=== Webapp Logs ==="; \
	$(COMPOSE_CMD) logs --tail=$(DOCKER_LOG_TAIL) webapp || true; \
	echo ""; \
	$(COMPOSE_CMD) down -v; \
	exit $$TEST_EXIT_CODE
