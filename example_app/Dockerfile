# syntax=docker/dockerfile:1.7

# Mirrors `tool.uv.required-version` in the root pyproject.toml; a
# mismatch fails loudly (uv refuses to run under required-version).
ARG UV_VERSION=0.12.3

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

FROM rust:1.88-bookworm AS rust-base
WORKDIR /repo
COPY rust-toolchain.toml .
RUN rustup toolchain install

FROM rust-base AS rust-base-chef
RUN cargo install cargo-chef --locked

FROM rust-base-chef AS rust-planner
WORKDIR /repo
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM rust-base-chef AS rust-builder
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    build-essential \
    libprotobuf-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

FROM rust-builder AS rust-cacher
COPY --from=rust-planner /repo/recipe.json recipe.json
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    cargo chef cook --release --recipe-path recipe.json

FROM rust-builder AS wheel-builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-venv \
    python3-pip \
    curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=uv /uv /uvx /usr/local/bin/

# Copy cached dependencies from cacher stage
COPY --from=rust-cacher /repo/target target

# Copy source files
COPY . .

# Build - only recompiles your code, not dependencies
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    --mount=type=cache,target=/root/.cache/uv \
    uv run --locked scripts/build_wheel.py --out-dir target/wheels

# =============================================================================
# Stage 4: Final runtime image
# =============================================================================
FROM python:3.12-bookworm AS runtime

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY --from=uv /uv /uvx /usr/local/bin/

WORKDIR /app
COPY example_app /app

RUN uv venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

COPY --from=wheel-builder /repo/target/wheels /tmp/wheels
RUN --mount=type=cache,target=/root/.cache/uv \
    set -eux; wheel=$(ls /tmp/wheels/waymark-*.whl | head -n 1); uv pip install --no-cache-dir "$wheel"
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --no-cache-dir --no-sources ".[tests]"

ENV WAYMARK_HTTP_PORT=24117 \
    WAYMARK_GRPC_PORT=24118

CMD ["uvicorn", "example_app.web:app", "--host", "0.0.0.0", "--port", "8000"]
