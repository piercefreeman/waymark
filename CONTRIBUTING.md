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
(`waymark-bridge`, `waymark-boot-singleton`, `waymark-executor`) inside the Python package, and invokes
`uv build --wheel` to produce an artifact suitable for publishing to PyPI.

### Local Server Runtime

The Rust runtime exposes a gRPC API (plus gRPC health check) via the `waymark-bridge` binary:

```bash
$ cargo run --bin waymark-bridge
```

It refuses to start on a database `waymark-executor` has not migrated, so run the executor against it first. After switching to a branch with new migrations, stop any `waymark-bridge` still running from before: the boot singleton reuses a running one, which never checks the schema again.

Developers can either launch it directly or rely on the `waymark-boot-singleton` helper which finds (or starts) a single shared instance on
`127.0.0.1:24117`. The helper prints the active gRPC port to stdout so Python clients can connect without additional
configuration:

```bash
$ cargo run --bin waymark-boot-singleton
24117
```

The Python bridge automatically shells out to the helper unless you set
`WAYMARK_BRIDGE_GRPC_PORT`; `WAYMARK_BRIDGE_GRPC_ADDR` alone only changes where it connects,
and the helper still runs. Once the port is known it opens a gRPC channel to the
`WorkflowService`.

### Benchmarking

Run the Rust benchmark harness (defaults to `--count 1000`) via:

```bash
$ make benchmark
```

`make benchmark` builds with `RUSTFLAGS="--cfg waymark_observability_chrome_trace"`, then
runs the benchmark once per value of `BENCH_CONCURRENCY_SWEEP` (default `25 100 250 1000`,
each as `WAYMARK_MAX_CONCURRENT_INSTANCES`). Each run writes a tracing-chrome file,
`target/benchmark-trace-<N>.json` by default, and prints a pyinstrument-style summary
via `scripts/parse_chrome_trace.py`. Override the trace path prefix
with `BENCH_TRACE=...` (the `.json` is dropped and `-<N>.json` appended), the sweep with
`BENCH_CONCURRENCY_SWEEP="100"`, the summary size with `BENCH_TRACE_TOP=...`, or benchmark args with
`BENCH_ARGS="--count 200 --base 10"`. Registration and persistence batch sizes are environment
knobs (`WAYMARK_REGISTRATION_BATCH_MAX`, `WAYMARK_SNAPSHOT_BATCH_MAX`, …), not CLI flags.
Set `BENCH_RELEASE=1` to run the benchmark binary
from the release profile. `make benchmark-trace` is an alias if you want the explicit target
name.

To inspect task waits and blocking points via tokio-console, use:

```bash
$ make benchmark-console
```

This opens a tmux session with the benchmark on the left and `tokio-console` on the right.
`make benchmark-console` requires tmux, and `tokio-console` must be installed (`cargo install
tokio-console --locked`). Tokio console also requires building with
`RUSTFLAGS="--cfg tokio_unstable --cfg waymark_observability_tokio_console"`, which the make
target sets by default (override with `BENCH_CONSOLE_RUSTFLAGS=...`). The console listens on `127.0.0.1:6669` by default; override with
`TOKIO_CONSOLE_BIND`. This is a tokio-console socket, not an HTTP endpoint, so it won’t
load in a browser. If tokio-console shows "RECONNECTING", reinstall it so the client/server
protocols match. We track the latest `console-subscriber` (0.5.x), while the CLI is still
0.1.x, so a stale install often causes reconnect loops.

For machine-readable results, pass `--json <file>` to `waymark-benchmark`; a bare `--json` prints
the JSON to stdout after the summary lines. Under `make benchmark`, a `--json <file>` in
`BENCH_ARGS` is overwritten by each run of the sweep.

## Testing

### Rust tests (unit + integration)

Integration fixtures are run by the Rust entrypoint binary `crates/bin/integration-test`.
It runs curated fixtures from `tests/integration_tests` and checks parity:

- Baseline execution via direct inline Python workflow logic
- Runtime execution via the VM in transient mode (in-memory, no persistence)
- Runtime execution via the VM in durable mode (Postgres-backed execution subsystem)
- Mode results must exactly match the inline baseline (result or error payload)

Commands:

```bash
# Unit and crate tests (the fixture parity runner below is a separate binary)
cargo test

# Run fixture integration parity (default modes: transient,durable)
cargo run --bin waymark-integration-test

# Run selected fixture case IDs only
cargo run --bin waymark-integration-test -- --case simple --case parallel

# Restrict parity modes (comma-separated)
cargo run --bin waymark-integration-test -- --modes transient
```

Prereqs:

- No manual Postgres startup is required for the default test harness configuration: the
  harness uses a Postgres on `127.0.0.1:5433`, and when none answers there it starts one with
  `docker compose up`, which needs Docker installed and running.
- Ensure `uv` is installed and `.venv` is prepared (`uv sync`)

### Python tests

```bash
cd python
uv run pytest
```
