# syntax=docker/dockerfile:1.7

FROM rust:1.88-bookworm AS rust-builder
WORKDIR /repo
RUN apt-get update && apt-get install -y --no-install-recommends \
    libprotobuf-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/repo/target \
    cargo build --release --bin waymark-bridge \
    && cp target/release/waymark-bridge /tmp/waymark-bridge

FROM node:20-bookworm AS javascript-builder
WORKDIR /repo
COPY package.json package-lock.json ./
COPY javascript/packages/nextjs/package.json javascript/packages/nextjs/
COPY examples/javascript/package.json examples/javascript/
RUN --mount=type=cache,target=/root/.npm npm ci --ignore-scripts
COPY javascript/packages/nextjs javascript/packages/nextjs
COPY examples/javascript examples/javascript
RUN npm run build --workspace @waymark/nextjs \
    && npm run build --workspace @waymark/example-javascript

FROM node:20-bookworm-slim AS runtime
WORKDIR /repo
ENV NODE_ENV=production \
    PATH="/repo/node_modules/.bin:${PATH}"
COPY package.json package-lock.json ./
COPY --from=javascript-builder /repo/node_modules node_modules
COPY --from=javascript-builder /repo/javascript javascript
COPY --from=javascript-builder /repo/examples/javascript examples/javascript
COPY --from=rust-builder /tmp/waymark-bridge /usr/local/bin/waymark-bridge
WORKDIR /repo/examples/javascript
EXPOSE 3000 24117
CMD ["npm", "run", "start"]
