# syntax=docker/dockerfile:1.7

# =============================================================================
# Stage 1: cargo-chef planner - analyze dependencies
# =============================================================================
FROM rust:1.88-bookworm AS planner
RUN cargo install cargo-chef --locked
WORKDIR /repo
COPY Cargo.toml Cargo.lock build.rs /repo/
COPY src /repo/src
COPY proto /repo/proto
RUN cargo chef prepare --recipe-path recipe.json

# =============================================================================
# Stage 2: cargo-chef cook - build dependencies (cached layer)
# =============================================================================
FROM rust:1.88-bookworm AS cacher

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    build-essential \
    libprotobuf-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

RUN cargo install cargo-chef --locked
WORKDIR /repo
COPY --from=planner /repo/recipe.json recipe.json
# Build dependencies - this layer is cached unless Cargo.toml/Cargo.lock change
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    cargo chef cook --release --recipe-path recipe.json

# =============================================================================
# Stage 3: Build the actual binaries (fast - deps already compiled)
# =============================================================================
FROM rust:1.88-bookworm AS wheel-builder

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-venv \
    python3-pip \
    curl \
    pkg-config \
    libssl-dev \
    build-essential \
    libprotobuf-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /repo

# Copy cached dependencies from cacher stage
COPY --from=cacher /repo/target target

# Copy source files
COPY Cargo.toml Cargo.lock build.rs README.md /repo/
COPY src /repo/src
COPY proto /repo/proto
COPY python /repo/python
COPY scripts /repo/scripts
COPY templates /repo/templates
COPY migrations /repo/migrations
COPY tests/fixtures/benchmark_workflow.py /repo/tests/fixtures/benchmark_workflow.py

# Build - only recompiles your code, not dependencies
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    --mount=type=cache,target=/root/.cache/uv \
    uv run scripts/build_wheel.py --out-dir target/wheels

# =============================================================================
# Stage 4: Final runtime image
# =============================================================================
FROM python:3.12-bookworm AS runtime

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /app
COPY example_app /app

# Copy templates for the webapp dashboard
COPY templates /app/templates

RUN uv venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

COPY --from=wheel-builder /repo/target/wheels /tmp/wheels
RUN --mount=type=cache,target=/root/.cache/uv \
    set -eux; wheel=$(ls /tmp/wheels/rappel-*.whl | head -n 1); uv pip install --no-cache-dir "$wheel"
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --no-cache-dir --no-sources ".[tests]"

ENV RAPPEL_HTTP_PORT=24117 \
    RAPPEL_GRPC_PORT=24118

CMD ["uvicorn", "example_app.web:app", "--host", "0.0.0.0", "--port", "8000"]
