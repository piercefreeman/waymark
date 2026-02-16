.PHONY: install
install:
	cd ../scripts && uv run python build_wheel.py --out-dir ../target/wheels
	uv sync --group dev
	uv pip install --force-reinstall $$(ls ../../target/wheels/waymark-*.whl | head -1)

CONTAINER := waymark-pg
PID_FILE := /tmp/waymark-workers.pid

define ENV
	PATH=.venv/bin:$$PATH PYTHONPATH=$(PWD)/.. \
	WAYMARK_DATABASE_URL=postgresql://postgres:pass@localhost:5432/postgres?sslmode=disable \
	WAYMARK_USER_MODULE=example_app.workflows
endef

.PHONY: run
run:
	docker rm -f $(CONTAINER) 2>/dev/null || true
	docker run -d --name $(CONTAINER) --rm -e POSTGRES_PASSWORD=pass -p 5432:5432 postgres:17-alpine >/dev/null
	until docker exec $(CONTAINER) pg_isready >/dev/null 2>&1; do sleep 0.1; done
	$(ENV) .venv/bin/start-workers >/dev/null 2>&1 & echo $$! > $(PID_FILE)
	sleep 1
	$(ENV) WAYMARK_RUN_REAL_CLUSTER=1 .venv/bin/pytest . -vvv; \
	TEST_EXIT=$$?; \
	kill $$(cat $(PID_FILE) 2>/dev/null) 2>/dev/null || true; \
	docker stop $(CONTAINER) >/dev/null 2>&1; \
	exit $$TEST_EXIT

.PHONY: clean
clean:
	rm -rf .venv ../../target/wheels $(PID_FILE)
	docker rm -f $(CONTAINER) 2>/dev/null || true
