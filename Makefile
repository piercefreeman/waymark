PY_PROTO_OUT := python/proto
PY_CORE_PROTO_OUT := core-python/proto

.PHONY: all build-proto clean lint lint-verify lint-extended lint-extended-verify python-lint python-lint-verify rust-lint rust-lint-verify rust-lint-extended rust-lint-extended-verify coverage python-coverage rust-coverage benchmark benchmark-console benchmark-console-run benchmark-trace

all: build-proto

build-proto:
	@mkdir -p $(PY_PROTO_OUT) $(PY_CORE_PROTO_OUT)
	cd python && uv run python -m grpc_tools.protoc \
		--proto_path=../proto \
		--plugin=protoc-gen-mypy="$$(pwd)/.venv/bin/protoc-gen-mypy" \
		--plugin=protoc-gen-mypy_grpc="$$(pwd)/.venv/bin/protoc-gen-mypy_grpc" \
		--python_out=../$(PY_PROTO_OUT) \
		--grpc_python_out=../$(PY_PROTO_OUT) \
		--mypy_out=../$(PY_PROTO_OUT) \
		--mypy_grpc_out=../$(PY_PROTO_OUT) \
		../proto/messages.proto ../proto/ast.proto
	cd python && uv run python -m grpc_tools.protoc \
		--proto_path=../proto \
		--plugin=protoc-gen-mypy="$$(pwd)/.venv/bin/protoc-gen-mypy" \
		--plugin=protoc-gen-mypy_grpc="$$(pwd)/.venv/bin/protoc-gen-mypy_grpc" \
		--python_out=../$(PY_CORE_PROTO_OUT) \
		--grpc_python_out=../$(PY_CORE_PROTO_OUT) \
		--mypy_out=../$(PY_CORE_PROTO_OUT) \
		--mypy_grpc_out=../$(PY_CORE_PROTO_OUT) \
		../proto/messages.proto ../proto/ast.proto
	cd python && uv run python ../scripts/fix_proto_imports.py
	$(MAKE) lint

clean:
	rm -rf target
	rm -rf $(PY_PROTO_OUT)
	rm -rf $(PY_CORE_PROTO_OUT)

lint: python-lint rust-lint

lint-verify: python-lint-verify rust-lint-verify

python-lint:
	cd python && uv run ruff format .
	cd python && uv run ruff check . --fix
	cd python && uv run ty check . --exclude proto/messages_pb2_grpc.py --extra-search-path proto
	cd scripts && uv run ruff format .
	cd scripts && uv run ruff check . --fix
	cd scripts && uv run ty check .

python-lint-verify:
	cd python && uv run ruff format --check .
	cd python && uv run ruff check .
	cd python && uv run ty check . --exclude proto/messages_pb2_grpc.py --extra-search-path proto
	cd scripts && uv run ruff format --check .
	cd scripts && uv run ruff check .
	cd scripts && uv run ty check .

rust-lint-base:
	cargo fmt
	cargo clippy --all-targets --all-features -- -D warnings

rust-lint-base-verify:
	cargo fmt -- --check
	cargo clippy --all-targets --all-features -- -D warnings

lint-extended: lint rust-lint-extended

lint-extended-verify: lint-verify rust-lint-extended-verify

rust-lint: rust-lint-base
	taplo fmt
	cargo shear
	typos
	cargo deny check

rust-lint-verify: rust-lint-base-verify
	taplo fmt --check
	cargo shear
	typos
	cargo deny check

rust-lint-extended:
	cargo hack clippy --feature-powerset --no-dev-deps --lib --workspace --exclude waymark-benchmark --exclude waymark-boot-singleton --exclude waymark-bridge --exclude waymark-integration-test --exclude waymark-smoke --exclude waymark-soak-harness --exclude waymark-start-workers -- -D warnings

rust-lint-extended-verify:
	cargo hack clippy --feature-powerset --no-dev-deps --lib --workspace --exclude waymark-benchmark --exclude waymark-boot-singleton --exclude waymark-bridge --exclude waymark-integration-test --exclude waymark-smoke --exclude waymark-soak-harness --exclude waymark-start-workers -- -D warnings

# Coverage targets
coverage: python-coverage rust-coverage

python-coverage:
	cd python && uv run pytest tests --cov=waymark --cov-report=term-missing --cov-report=xml:coverage.xml --cov-report=html:htmlcov

rust-coverage:
	cargo llvm-cov --lcov --output-path target/rust-coverage.lcov
	cargo llvm-cov --html --output-dir target/rust-htmlcov

BENCH_ARGS ?= --count 1000
BENCH_TRACE ?= target/benchmark-trace.json
BENCH_TRACE_PREFIX ?= $(BENCH_TRACE:.json=)
BENCH_TRACE_TOP ?= 30
BENCH_CONSOLE_BIND ?= 127.0.0.1:6669
BENCH_CONSOLE_ARGS ?= --observe
BENCH_CONSOLE_RUSTFLAGS ?= --cfg waymark_observability_trace
BENCH_RUSTFLAGS ?= --cfg tokio_unstable --cfg waymark_observability_trace
BENCH_CONCURRENCY_SWEEP ?= 25 100 250 1000
BENCH_RELEASE ?= 0
BENCH_PROFILE_FLAG := $(if $(filter 1 true yes,$(BENCH_RELEASE)),--release,)
BENCH_BIN := target/$(if $(filter 1 true yes,$(BENCH_RELEASE)),release,debug)/waymark-benchmark
benchmark: benchmark-trace

benchmark-console:
	tmux has-session -t waymark-benchmark 2>/dev/null && tmux kill-session -t waymark-benchmark || true
	tmux new-session -d -s waymark-benchmark 'bash -lc "make benchmark-console-run; exec $$SHELL"'
	tmux split-window -h -t waymark-benchmark 'bash -lc "TOKIO_CONSOLE_BIND=$(BENCH_CONSOLE_BIND) tokio-console || { echo \"tokio-console not found (run: cargo install tokio-console)\"; exec $$SHELL; }"'
	tmux select-layout -t waymark-benchmark even-horizontal
	tmux select-pane -t waymark-benchmark:0.0
	tmux attach -t waymark-benchmark

benchmark-console-run:
	TOKIO_CONSOLE_BIND="$(BENCH_CONSOLE_BIND)" RUSTFLAGS="$(BENCH_RUSTFLAGS) $(BENCH_CONSOLE_RUSTFLAGS)" cargo build $(BENCH_PROFILE_FLAG) --bin waymark-benchmark
	TOKIO_CONSOLE_BIND="$(BENCH_CONSOLE_BIND)" RUSTFLAGS="$(BENCH_RUSTFLAGS) $(BENCH_CONSOLE_RUSTFLAGS)" $(BENCH_BIN) $(BENCH_CONSOLE_ARGS) $(BENCH_ARGS)

benchmark-trace:
	RUSTFLAGS="$(BENCH_RUSTFLAGS)" cargo build $(BENCH_PROFILE_FLAG) --bin waymark-benchmark
	@for max in $(BENCH_CONCURRENCY_SWEEP); do \
		trace_file="$(BENCH_TRACE_PREFIX)-$${max}.json"; \
		echo "=== BENCH: max_concurrent_instances=$${max} ==="; \
		WAYMARK_MAX_CONCURRENT_INSTANCES=$${max} $(BENCH_BIN) --trace $$trace_file $(BENCH_ARGS); \
		uv run python scripts/parse_chrome_trace.py $$trace_file --top $(BENCH_TRACE_TOP); \
	done
