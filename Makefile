PROTO_FILES := proto/messages.proto
PY_PROTO_OUT := python/proto

.PHONY: all build-proto clean lint

all: build-proto

build-proto:
	@mkdir -p $(PY_PROTO_OUT)
	protoc --proto_path=proto --python_out=$(PY_PROTO_OUT) $(PROTO_FILES)

clean:
	rm -rf target
	rm -rf $(PY_PROTO_OUT)

lint:
	uvx ruff format python
	uvx ruff check python --fix
	uvx --with protobuf ty check python
	cargo fmt
	cargo clippy -- -D warnings
