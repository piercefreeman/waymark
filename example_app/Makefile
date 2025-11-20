IMAGE_NAME ?= rappel-example-app
COMPOSE_CMD ?= docker compose -f docker-compose.yml

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
	@echo "Running tests in docker-compose environment"
	$(COMPOSE_CMD) up -d postgres daemons
	@echo "Waiting for postgres to be healthy..."
	@timeout 30 sh -c 'until docker compose exec postgres pg_isready -U rappel > /dev/null 2>&1; do sleep 1; done' || (echo "Postgres failed to become healthy" && exit 1)
	@echo "Waiting for workers to be ready..."
	@sleep 3
	@set -e; \
	TEST_EXIT_CODE=0; \
	$(COMPOSE_CMD) run --rm -e DATABASE_URL=postgresql://rappel:rappel@postgres:5432/rappel_example webapp uv run --active pytest -vvv || TEST_EXIT_CODE=$$?; \
	echo ""; \
	echo "=== Daemon Logs ==="; \
	$(COMPOSE_CMD) logs daemons || true; \
	echo ""; \
	echo "=== Webapp Logs ==="; \
	$(COMPOSE_CMD) logs webapp || true; \
	echo ""; \
	$(COMPOSE_CMD) down -v; \
	exit $$TEST_EXIT_CODE
