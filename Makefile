PROTO_FILES := proto/messages.proto
PY_PROTO_OUT := python/proto

.PHONY: all build-proto clean lint lint-verify python-lint python-lint-verify rust-lint rust-lint-verify

all: build-proto

build-proto:
	@mkdir -p $(PY_PROTO_OUT)
	cd python && uv run python -m grpc_tools.protoc \
		--proto_path=../proto \
		--python_out=../$(PY_PROTO_OUT) \
		--grpc_python_out=../$(PY_PROTO_OUT) \
		--mypy_out=../$(PY_PROTO_OUT) \
		--mypy_grpc_out=../$(PY_PROTO_OUT) \
		../$(PROTO_FILES)
	@python scripts/fix_proto_imports.py

clean:
	rm -rf target
	rm -rf $(PY_PROTO_OUT)

lint: python-lint rust-lint

lint-verify: python-lint-verify rust-lint-verify

python-lint:
	cd python && uv run ruff format .
	cd python && uv run ruff check . --fix
	cd python && uv run ty check . --exclude proto/messages_pb2_grpc.py

python-lint-verify:
	cd python && uv run ruff format --check .
	cd python && uv run ruff check .
	cd python && uv run ty check . --exclude proto/messages_pb2_grpc.py

rust-lint:
	cargo fmt
	cargo clippy --all-targets --all-features -- -D warnings

rust-lint-verify:
	cargo fmt -- --check
	cargo clippy --all-targets --all-features -- -D warnings
