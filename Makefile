PROTO_FILES := proto/messages.proto proto/ir.proto
PY_PROTO_OUT := python/proto

.PHONY: all build-proto clean lint lint-verify python-lint python-lint-verify rust-lint rust-lint-verify coverage python-coverage rust-coverage

all: build-proto

build-proto:
	@mkdir -p $(PY_PROTO_OUT)
	cd python && uv run python -m grpc_tools.protoc \
		--proto_path=../proto \
		--python_out=../$(PY_PROTO_OUT) \
		--grpc_python_out=../$(PY_PROTO_OUT) \
		--mypy_out=../$(PY_PROTO_OUT) \
		--mypy_grpc_out=../$(PY_PROTO_OUT) \
		$(addprefix ../,$(PROTO_FILES))
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
	cd scripts && uv run ty check .

python-lint-verify:
	cd python && uv run ruff format --check .
	cd python && uv run ruff check .
	cd python && uv run ty check . --exclude proto/messages_pb2_grpc.py
	cd scripts && uv run ruff format --check .
	cd scripts && uv run ruff check .
	cd scripts && uv run ty check .

rust-lint:
	cargo fmt
	cargo clippy --all-targets --all-features -- -D warnings

rust-lint-verify:
	cargo fmt -- --check
	cargo clippy --all-targets --all-features -- -D warnings

# Coverage targets
# Note: Rust --branch flag requires nightly toolchain. We detect if nightly is available
# and add --branch automatically. Install nightly with: rustup toolchain install nightly --component llvm-tools-preview
RUST_HAS_NIGHTLY := $(shell rustup run nightly rustc --version 2>/dev/null && echo 1 || echo 0)
RUST_BRANCH_FLAG := $(if $(filter 1,$(RUST_HAS_NIGHTLY)),--branch,)

coverage: python-coverage rust-coverage

python-coverage:
	cd python && uv run pytest tests --cov=rappel --cov-report=term-missing --cov-report=xml:coverage.xml --cov-report=html:htmlcov

rust-coverage:
	cargo $(if $(filter 1,$(RUST_HAS_NIGHTLY)),+nightly,) llvm-cov $(RUST_BRANCH_FLAG) --lcov --output-path target/rust-coverage.lcov
	cargo $(if $(filter 1,$(RUST_HAS_NIGHTLY)),+nightly,) llvm-cov $(RUST_BRANCH_FLAG) --html --output-dir target/rust-htmlcov
