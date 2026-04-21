# syntax=docker/dockerfile:1.7

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

COPY --from=rust-planner /repo/recipe.json recipe.json
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    cargo chef cook --release --recipe-path recipe.json

COPY . .
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    cargo build --release \
      --bin waymark-boot-singleton \
      --bin waymark-bridge \
      --bin waymark-start-workers

FROM node:24-bookworm AS node-builder
WORKDIR /repo

COPY javascript/packages/nextjs /repo/javascript/packages/nextjs
WORKDIR /repo/javascript/packages/nextjs
RUN npm ci

COPY example-app/javascript/package.json /repo/example-app/javascript/package.json
COPY example-app/javascript/package-lock.json /repo/example-app/javascript/package-lock.json

WORKDIR /repo/example-app/javascript
RUN npm ci

COPY example-app/javascript /repo/example-app/javascript
RUN npm run build

FROM node:24-bookworm AS runtime
ENV NODE_ENV=production
WORKDIR /app

COPY --from=rust-builder /repo/target/release/waymark-boot-singleton /usr/local/bin/waymark-boot-singleton
COPY --from=rust-builder /repo/target/release/waymark-bridge /usr/local/bin/waymark-bridge
COPY --from=rust-builder /repo/target/release/waymark-start-workers /usr/local/bin/waymark-start-workers
COPY --from=node-builder /repo/javascript/packages/nextjs /app/javascript/packages/nextjs
COPY --from=node-builder /repo/example-app/javascript /app/example-app/javascript

ENV PATH="/app/javascript/packages/nextjs/node_modules/.bin:/app/example-app/javascript/node_modules/.bin:${PATH}"

WORKDIR /app/example-app/javascript
CMD ["npm", "run", "start:docker"]
