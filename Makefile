PY_PROTO_OUT := python/proto

.PHONY: all build-proto clean lint lint-verify python-lint python-lint-verify rust-lint rust-lint-verify coverage python-coverage rust-coverage

all: build-proto

build-proto:
	@mkdir -p $(PY_PROTO_OUT)
	cd python && uv run python -m grpc_tools.protoc \
		--proto_path=../proto \
		--plugin=protoc-gen-mypy="$$(pwd)/.venv/bin/protoc-gen-mypy" \
		--plugin=protoc-gen-mypy_grpc="$$(pwd)/.venv/bin/protoc-gen-mypy_grpc" \
		--python_out=../$(PY_PROTO_OUT) \
		--grpc_python_out=../$(PY_PROTO_OUT) \
		--mypy_out=../$(PY_PROTO_OUT) \
		--mypy_grpc_out=../$(PY_PROTO_OUT) \
		../proto/messages.proto ../proto/ast.proto
	@python scripts/fix_proto_imports.py
	$(MAKE) lint

clean:
	rm -rf target
	rm -rf $(PY_PROTO_OUT)

lint: python-lint rust-lint

lint-verify: python-lint-verify rust-lint-verify

python-lint:
	cd python && uv run ruff format .
	cd python && uv run ruff check . --fix
	cd python && uv run ty check . --exclude proto/messages_pb2_grpc.py
	cd scripts && uv run ruff format .
	cd scripts && uv run ruff check . --fix
	cd scripts && PYTHONPATH=../python/src:../python uv run ty check .

python-lint-verify:
	cd python && uv run ruff format --check .
	cd python && uv run ruff check .
	cd python && uv run ty check . --exclude proto/messages_pb2_grpc.py
	cd scripts && uv run ruff format --check .
	cd scripts && uv run ruff check .
	cd scripts && PYTHONPATH=../python/src:../python uv run ty check .

rust-lint:
	cargo fmt
	cargo clippy --all-targets --all-features -- -D warnings

rust-lint-verify:
	cargo fmt -- --check
	cargo clippy --all-targets --all-features -- -D warnings

# Coverage targets
coverage: python-coverage rust-coverage

python-coverage:
	cd python && uv run pytest tests --cov=rappel --cov-report=term-missing --cov-report=xml:coverage.xml --cov-report=html:htmlcov

rust-coverage:
	cargo llvm-cov --lcov --output-path target/rust-coverage.lcov
	cargo llvm-cov --html --output-dir target/rust-htmlcov
