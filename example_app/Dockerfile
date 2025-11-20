# syntax=docker/dockerfile:1.7

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
COPY . /repo
RUN uv run scripts/build_wheel.py --out-dir target/wheels

FROM python:3.12-slim-bookworm AS runtime

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /app
COPY example_app /app

RUN uv venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

COPY --from=wheel-builder /repo/target/wheels /tmp/wheels
RUN set -eux; wheel=$(ls /tmp/wheels/rappel-*.whl | head -n 1); uv pip install --no-cache-dir "$wheel"
RUN uv pip install --no-cache-dir ".[tests]"

RUN chmod +x scripts/start-daemons.sh

ENV CARABINER_HTTP_PORT=24117 \
    CARABINER_GRPC_PORT=24118

CMD ["uvicorn", "example_app.web:app", "--host", "0.0.0.0", "--port", "8000"]
