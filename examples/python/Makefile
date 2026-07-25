IMAGE_NAME ?= waymark-example-app
COMPOSE_CMD ?= docker compose -f docker-compose.yml
DOCKER_TEST_TIMEOUT ?= 300
DOCKER_LOG_TAIL ?= 200
DOCKER_LOG_TIMEOUT ?= 30
DOCKER_CLEANUP_TIMEOUT ?= 45

.PHONY: build up down logs docker-test

build:
	@echo "Building $(IMAGE_NAME) image"
	docker build -f Dockerfile -t $(IMAGE_NAME) ../..

up:
	$(COMPOSE_CMD) up --build -d

logs:
	$(COMPOSE_CMD) logs -f

down:
	$(COMPOSE_CMD) down -v

docker-test:
	@echo "Running tests in docker-compose environment (timeout $(DOCKER_TEST_TIMEOUT)s)"
	@timeout $(DOCKER_CLEANUP_TIMEOUT) $(COMPOSE_CMD) down -v --remove-orphans >/dev/null 2>&1 || true
	@HOST_ARCH="$$(docker version --format '{{.Server.Arch}}' 2>/dev/null || true)"; \
	IMAGE_ARCH="$$(docker image inspect postgres:16-alpine --format '{{.Architecture}}' 2>/dev/null || true)"; \
	if [ -n "$$HOST_ARCH" ] && [ -n "$$IMAGE_ARCH" ] && [ "$$HOST_ARCH" != "$$IMAGE_ARCH" ]; then \
		echo "Refreshing postgres:16-alpine for host arch $$HOST_ARCH (found $$IMAGE_ARCH)"; \
		docker image rm postgres:16-alpine >/dev/null 2>&1 || true; \
	fi; \
	if ! docker image inspect postgres:16-alpine >/dev/null 2>&1; then \
		case "$$HOST_ARCH" in \
			arm64) PLATFORM="linux/arm64/v8" ;; \
			amd64) PLATFORM="linux/amd64" ;; \
			*) PLATFORM="" ;; \
		esac; \
		if [ -n "$$PLATFORM" ]; then \
			docker pull --platform "$$PLATFORM" postgres:16-alpine >/dev/null; \
		else \
			docker pull postgres:16-alpine >/dev/null; \
		fi; \
	fi
	$(COMPOSE_CMD) up -d postgres daemons
	@echo "Waiting for postgres to be healthy..."
	@timeout 60 sh -c 'until cid="$$($(COMPOSE_CMD) ps -q postgres 2>/dev/null)" && [ -n "$$cid" ] && [ "$$(docker inspect -f "{{.State.Health.Status}}" "$$cid" 2>/dev/null)" = "healthy" ]; do sleep 1; done' || (echo "Postgres failed to become healthy" && $(COMPOSE_CMD) logs postgres || true; exit 1)
	@echo "Waiting for workers to be ready..."
	@sleep 3
	@echo "Checking that daemons are running and ready..."
	@timeout 60 sh -c 'until $(COMPOSE_CMD) ps --status=running --services | grep -q "^daemons$$"; do sleep 1; done' || (echo "Daemons failed to stay running" && $(COMPOSE_CMD) logs daemons || true; $(COMPOSE_CMD) down -v; exit 1)
	@timeout 120 sh -c 'until $(COMPOSE_CMD) logs --no-color daemons 2>&1 | grep -q "worker pool ready"; do sleep 1; done' || (echo "Daemons never reported worker pool ready" && $(COMPOSE_CMD) logs daemons || true; $(COMPOSE_CMD) down -v; exit 1)
	@set -e; \
	TEST_EXIT_CODE=0; \
	timeout $(DOCKER_TEST_TIMEOUT) $(COMPOSE_CMD) run --no-deps --interactive=false -T --rm -e WAYMARK_DATABASE_URL=postgresql://waymark:waymark@postgres:5432/waymark_example webapp pytest -vvv || TEST_EXIT_CODE=$$?; \
	if [ "$$TEST_EXIT_CODE" -eq 124 ]; then \
		echo "Docker test run timed out after $(DOCKER_TEST_TIMEOUT)s"; \
	fi; \
	echo "Docker test command exit code: $$TEST_EXIT_CODE"; \
	echo ""; \
	echo "=== Daemon Logs ==="; \
	timeout $(DOCKER_LOG_TIMEOUT) $(COMPOSE_CMD) logs --tail=$(DOCKER_LOG_TAIL) daemons || true; \
	echo ""; \
	echo "=== Webapp Logs ==="; \
	timeout $(DOCKER_LOG_TIMEOUT) $(COMPOSE_CMD) logs --tail=$(DOCKER_LOG_TAIL) webapp || true; \
	echo ""; \
	CLEANUP_EXIT_CODE=0; \
	for attempt in 1 2 3; do \
		if timeout $(DOCKER_CLEANUP_TIMEOUT) $(COMPOSE_CMD) down -v; then \
			CLEANUP_EXIT_CODE=0; \
			break; \
		fi; \
		CLEANUP_EXIT_CODE=$$?; \
		echo "docker compose down failed or timed out (attempt $$attempt, exit $$CLEANUP_EXIT_CODE), retrying..."; \
		sleep 1; \
	done; \
	if [ "$$CLEANUP_EXIT_CODE" -ne 0 ] && [ "$$TEST_EXIT_CODE" -eq 0 ]; then \
		TEST_EXIT_CODE=$$CLEANUP_EXIT_CODE; \
	fi; \
	exit $$TEST_EXIT_CODE
