# Contributing

[![DeepWiki](https://img.shields.io/badge/DeepWiki-piercefreeman%2Fwaymark-blue.svg?logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACwAAAAyCAYAAAAnWDnqAAAAAXNSR0IArs4c6QAAA05JREFUaEPtmUtyEzEQhtWTQyQLHNak2AB7ZnyXZMEjXMGeK/AIi+QuHrMnbChYY7MIh8g01fJoopFb0uhhEqqcbWTp06/uv1saEDv4O3n3dV60RfP947Mm9/SQc0ICFQgzfc4CYZoTPAswgSJCCUJUnAAoRHOAUOcATwbmVLWdGoH//PB8mnKqScAhsD0kYP3j/Yt5LPQe2KvcXmGvRHcDnpxfL2zOYJ1mFwrryWTz0advv1Ut4CJgf5uhDuDj5eUcAUoahrdY/56ebRWeraTjMt/00Sh3UDtjgHtQNHwcRGOC98BJEAEymycmYcWwOprTgcB6VZ5JK5TAJ+fXGLBm3FDAmn6oPPjR4rKCAoJCal2eAiQp2x0vxTPB3ALO2CRkwmDy5WohzBDwSEFKRwPbknEggCPB/imwrycgxX2NzoMCHhPkDwqYMr9tRcP5qNrMZHkVnOjRMWwLCcr8ohBVb1OMjxLwGCvjTikrsBOiA6fNyCrm8V1rP93iVPpwaE+gO0SsWmPiXB+jikdf6SizrT5qKasx5j8ABbHpFTx+vFXp9EnYQmLx02h1QTTrl6eDqxLnGjporxl3NL3agEvXdT0WmEost648sQOYAeJS9Q7bfUVoMGnjo4AZdUMQku50McDcMWcBPvr0SzbTAFDfvJqwLzgxwATnCgnp4wDl6Aa+Ax283gghmj+vj7feE2KBBRMW3FzOpLOADl0Isb5587h/U4gGvkt5v60Z1VLG8BhYjbzRwyQZemwAd6cCR5/XFWLYZRIMpX39AR0tjaGGiGzLVyhse5C9RKC6ai42ppWPKiBagOvaYk8lO7DajerabOZP46Lby5wKjw1HCRx7p9sVMOWGzb/vA1hwiWc6jm3MvQDTogQkiqIhJV0nBQBTU+3okKCFDy9WwferkHjtxib7t3xIUQtHxnIwtx4mpg26/HfwVNVDb4oI9RHmx5WGelRVlrtiw43zboCLaxv46AZeB3IlTkwouebTr1y2NjSpHz68WNFjHvupy3q8TFn3Hos2IAk4Ju5dCo8B3wP7VPr/FGaKiG+T+v+TQqIrOqMTL1VdWV1DdmcbO8KXBz6esmYWYKPwDL5b5FA1a0hwapHiom0r/cKaoqr+27/XcrS5UwSMbQAAAABJRU5ErkJggg==)](https://deepwiki.com/piercefreeman/waymark)

## Development

### Packaging

Use the helper script to produce distributable wheels that bundle the Rust executables with the
Python package:

```bash
$ uv run scripts/build_wheel.py --out-dir target/wheels
```

The script compiles every Rust binary (release profile), stages the required entrypoints
(`waymark-bridge`, `boot-waymark-singleton`) inside the Python package, and invokes
`uv build --wheel` to produce an artifact suitable for publishing to PyPI.

### Local Server Runtime

The Rust runtime exposes a gRPC API (plus gRPC health check) via the `waymark-bridge` binary:

```bash
$ cargo run --bin waymark-bridge
```

Developers can either launch it directly or rely on the `boot-waymark-singleton` helper which finds (or starts) a single shared instance on
`127.0.0.1:24117`. The helper prints the active gRPC port to stdout so Python clients can connect without additional
configuration:

```bash
$ cargo run --bin boot-waymark-singleton
24117
```

The Python bridge automatically shells out to the helper unless you provide
`WAYMARK_BRIDGE_GRPC_ADDR` (or `WAYMARK_BRIDGE_GRPC_HOST` + `WAYMARK_BRIDGE_GRPC_PORT`) overrides.
Once the port is known it opens a gRPC channel to the
`WorkflowService`.

### Benchmarking

Run the Rust benchmark harness (defaults to `--count 1000`) via:

```bash
$ make benchmark
```

`make benchmark` builds with `--features trace`, writes a tracing-chrome file, and prints
a pyinstrument-style summary via `scripts/parse_chrome_trace.py`. Override the trace path
with `BENCH_TRACE=...`, the summary size with `BENCH_TRACE_TOP=...`, or benchmark args with
`BENCH_ARGS="--count 200 --batch-size 50"`. Set `BENCH_RELEASE=1` to run the benchmark binary
from the release profile. `make benchmark-trace` is an alias if you want the explicit target
name.

To inspect task waits and blocking points via tokio-console, use:

```bash
$ make benchmark-console
```

This opens a tmux session with the benchmark on the left and `tokio-console` on the right.
`make benchmark-console` requires tmux, and `tokio-console` must be installed (`cargo install
tokio-console --locked`). Tokio console also requires building with
`RUSTFLAGS="--cfg tokio_unstable"`, which the make target sets by default (override with
`BENCH_RUSTFLAGS=...`). The console listens on `127.0.0.1:6669` by default; override with
`TOKIO_CONSOLE_BIND`. This is a tokio-console socket, not an HTTP endpoint, so it won’t
load in a browser. If tokio-console shows "RECONNECTING", reinstall it so the client/server
protocols match. We track the latest `console-subscriber` (0.5.x), while the CLI is still
0.1.x, so a stale install often causes reconnect loops.

Stream benchmark output directly into our parser to summarize throughput and latency samples:

```bash
$ cargo run --bin bench -- \
  --messages 100000 \
  --payload 1024 \
  --concurrency 64 \
  --workers 4 \
  --log-interval 15 \
  uv run python/tools/parse_bench_logs.py

The `bench` binary seeds raw actions to measure dequeue/execute/ack throughput. Use `bench_instances` for an end-to-end workflow run (queueing and executing full workflow instances via the scheduler) without installing a separate `waymark-worker` binary—the harness shells out to `uv run python -m waymark.worker` automatically:

```bash
$ cargo run --bin bench_instances -- \
  --instances 200 \
  --batch-size 4 \
  --payload-size 1024 \
  --concurrency 64 \
  --workers 4
```

Add `--json` to the parser if you prefer JSON output.

## Testing

### Rust tests (unit + integration)

Integration fixtures are run by the Rust entrypoint binary `src/bin/integration_test.rs`.
It runs curated fixtures from `tests/integration_tests` and checks parity:
- Baseline execution via direct inline Python workflow logic
- Runtime execution via Rust DAG execution + in-memory backend
- Runtime execution via Rust DAG execution + Postgres backend
- Backend results must exactly match the inline baseline (result or error payload)

Commands:

```bash
# Everything (unit + integration)
cargo test

# Run fixture integration parity (default backends: in-memory,postgres)
cargo run --bin integration_test

# Run selected fixture case IDs only
cargo run --bin integration_test -- --case simple --case parallel

# Restrict parity backends (comma-separated)
cargo run --bin integration_test -- --backends in-memory
```

Prereqs:
- No manual Postgres startup is required for the default test harness configuration.
- Ensure `uv` is installed and `python/.venv` is prepared (`cd python && uv sync`)

### Python tests

```bash
cd python
uv run pytest
```
