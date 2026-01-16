# syntax=docker/dockerfile:1.7

FROM rust:1.88-bookworm AS rust-builder

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    libprotobuf-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /repo
COPY . /repo
RUN cargo build --release --bin rappel-bridge --bin start-workers

FROM node:20-bookworm-slim AS node-builder

WORKDIR /repo
COPY js /repo/js
COPY proto /repo/proto
COPY example_app_js /repo/example_app_js

RUN cd /repo/js && npm install && npm run build && npm pack --silent
RUN cd /repo/example_app_js && npm install
RUN rm -rf /repo/example_app_js/node_modules/@rappel/js
RUN cd /repo/example_app_js && npm install --no-save ../js/*.tgz
RUN cd /repo/example_app_js && npm run build

FROM node:20-bookworm-slim AS runtime

ENV NODE_ENV=production

WORKDIR /app

COPY --from=rust-builder /repo/target/release/rappel-bridge /usr/local/bin/rappel-bridge
COPY --from=rust-builder /repo/target/release/start-workers /usr/local/bin/start-workers

COPY proto /app/proto

COPY --from=node-builder /repo/example_app_js /app/example_app_js

COPY example_app_js/scripts/rappel-worker /usr/local/bin/rappel-worker
RUN chmod +x /usr/local/bin/rappel-worker

EXPOSE 8001 24117 24118 24119

WORKDIR /app/example_app_js
CMD ["node", "dist/server.js"]
