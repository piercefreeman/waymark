# waymark

![Waymark Logo](https://raw.githubusercontent.com/piercefreeman/waymark/main/media/header.png)

waymark is a library to let you build durable background tasks that withstand server restarts, task crashes, and long-running jobs. It's built for Python and Postgres without any additional deploy time requirements.

## Getting Started

We ship all client and server wheels as a python package. Install it via your package manager of choice:

```bash
uv add waymark
```

Once installed, Waymark exposes `start-workers` as a runnable bin entrypoint in your environment.
You can boot the worker pool directly with `uv run`:

```bash
export WAYMARK_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/waymark
uv run start-workers
```

## Usage

Let's say you need to send welcome emails to a batch of users, but only the active ones. You want to fetch them all, filter out inactive accounts, then fan out emails in parallel. This is how you write that workflow in waymark:

```python
import asyncio
from waymark import Workflow, action, workflow

@workflow
class WelcomeEmailWorkflow(Workflow):
    async def run(self, user_ids: list[str]) -> list[EmailResult]:
        users = await fetch_users(user_ids)
        active_users = [user for user in users if user.active]

        results = await asyncio.gather(
            *[
                send_email(to=user.email, subject="Welcome")
                for user in active_users
            ],
            return_exceptions=True,
        )
        
        return results
```

And here's how you define the actions distributed to your worker cluster:

```python
@action
async def fetch_users(
    user_ids: list[str],
    db: Annotated[Database, Depend(get_db)],
) -> list[User]:
    return await db.get_many(User, user_ids)

@action
async def send_email(
    to: str,
    subject: str,
    emailer: Annotated[EmailClient, Depend(get_email_client)],
) -> EmailResult:
    return await emailer.send(to=to, subject=subject)
```

To kick off a background job and wait for completion:

```python
async def welcome_users(user_ids: list[str]):
    workflow = WelcomeEmailWorkflow()
    await workflow.run(user_ids)
```

When you call `await workflow.run()`, we parse the AST of your `run()` method and compile it into the Waymark Runtime Language. The `for` loop becomes a filter node, the `asyncio.gather` becomes a parallel fan-out. None of this executes inline in your webserver, instead it's queued to Postgres and orchestrated by the Rust runtime across your worker cluster.

**Actions** are the distributed work: network calls, database queries, anything that can fail and should be retried independently.

**Workflows** are the control flow: loops, conditionals, parallel branches. They orchestrate actions but don't do heavy lifting themselves.

### Complex Workflows

Workflows can get much more complex than the example above:

1. Customizable retry policy

    By default your Python code will execute like native logic would: any exceptions will throw and immediately fail. Actions are set to timeout after ~5min to keep the queues from backing up - although we will continuously retry timed out actions in case they were caused by a failed node in your cluster. If you want to control this logic to be more robust, you can set retry policies and backoff intervals so you can attempt the action multiple times until it succeeds.

    ```python
    from waymark import RetryPolicy, BackoffPolicy
    from datetime import timedelta

    async def run(self):
        await self.run_action(
            inconsistent_action(0.5),
            # control handling of failures
            retry=RetryPolicy(attempts=50),
            backoff=BackoffPolicy(base_delay=5),
            timeout=timedelta(minutes=10)
        )
    ```

1. Branching control flows

    Use if statements, for loops, or any other Python primitives within the control logic. We will automatically detect these branches and compile them into a DAG node that gets executed just like your other actions.

    ```python
    async def run(self, user_id: str) -> Summary:
        # loop + non-action helper call
        top_spenders: list[float] = []
        for record in summary.transactions.records:
            if record.is_high_value:
                top_spenders.append(record.amount)
    ```

1. asyncio primitives

    Use asyncio.gather to parallelize tasks. Use asyncio.sleep to sleep for a longer period of time.

    ```python
    import asyncio

    async def run(self, user_id: str) -> Summary:
        # parallelize independent actions with gather
        profile, settings, history = await asyncio.gather(
            fetch_profile(user_id=user_id),
            fetch_settings(user_id=user_id),
            fetch_purchase_history(user_id=user_id),
            return_exceptions=True,
        ) 

        # wait before sending email
        await asyncio.sleep(24*60*60)
        recommendations = await email_ping(history)

        return Summary(profile=profile, settings=settings, recommendations=recommendations)
    ```

### Error handling

To build truly robust background tasks, you need to consider how things can go wrong. Actions can 'fail' in a couple ways. This is supported by our `.run_action` syntax that allows users to provide additional parameters to modify the execution bounds on each action.

1. Action explicitly throws an error and we want to retry it. Caused by intermittent database connectivity / overloaded webservers / or simply buggy code will throw an error. This comes from a standard python `raise Exception()`
1. Actions raise an error that is a really a WaymarkTimeout. This indicates that we dequeued the task but weren't able to complete it in the time allocated. This could be because we dequeued the task, started work on it, then the server crashed. Or it could still be running in the background but simply took too much time. Either way we will raise a synthetic error that is representative of this execution.

By default we will only try explicit actions one time if there is an explicit exception raised. We will try them infinite times in the case of a timeout since this is usually caused by cross device coordination issues.

## Project Status

> [!IMPORTANT]
> Right now you shouldn't use waymark in any production applications. The spec is changing too quickly and we don't guarantee backwards compatibility before 1.0.0. But we would love if you try it out in your side project and see how you find it.

Waymark is in an early alpha. Particular areas of focus include:

1. Finalizing the Waymark Runtime Language
1. Extending AST parsing logic to handle most core control flows
1. Performance tuning
1. Unit and integration tests

If you have a particular workflow that you think should be working but isn't yet producing the correct DAG (you can visualize it via CLI by `.visualize()`) please file an issue.

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

## Configuration

Waymark runtime configuration is environment-variable driven.
Waymark reads the process environment directly; it does not auto-load `.env` files.

### `start-workers` runtime

#### Commonly customized

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `WAYMARK_DATABASE_URL` | PostgreSQL DSN for worker runtime state/backend | required |
| `WAYMARK_WORKER_COUNT` | Number of Python worker processes | host CPU count (`available_parallelism`) |
| `WAYMARK_CONCURRENT_PER_WORKER` | Max concurrent actions per Python worker | `10` |
| `WAYMARK_MAX_CONCURRENT_INSTANCES` | Max in-memory instances across runloop shards | `500` |
| `WAYMARK_EXECUTOR_SHARDS` | Number of executor shards | host CPU count (`available_parallelism`) |
| `WAYMARK_USER_MODULE` | Comma-separated Python modules preloaded in workers | unset |
| `WAYMARK_MAX_ACTION_LIFECYCLE` | Max actions per worker before worker recycle | unset (no recycle limit) |
| `WAYMARK_WEBAPP_ENABLED` | Enable embedded webapp | `false` |
| `WAYMARK_WEBAPP_ADDR` | Webapp bind address | `0.0.0.0:24119` |

#### Advanced tuning

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `WAYMARK_WORKER_GRPC_ADDR` | gRPC bind addr used by the Python worker bridge server | `127.0.0.1:24118` |
| `WAYMARK_POLL_INTERVAL_MS` | Queue poll interval for runloop | `100` |
| `WAYMARK_INSTANCE_DONE_BATCH_SIZE` | Batch size for persisting completed instances | unset (uses `WAYMARK_MAX_CONCURRENT_INSTANCES`) |
| `WAYMARK_PERSIST_INTERVAL_MS` | Persistence flush interval | `500` |
| `WAYMARK_LOCK_TTL_MS` | Queue lock TTL | `15000` |
| `WAYMARK_LOCK_HEARTBEAT_MS` | Queue lock heartbeat interval | `5000` |
| `WAYMARK_EVICT_SLEEP_THRESHOLD_MS` | Sleep threshold for evicting idle instances from memory | `10000` |
| `WAYMARK_EXPIRED_LOCK_RECLAIMER_INTERVAL_MS` | Expired lock reclaim sweep interval | `1000` (clamped to min `1`) |
| `WAYMARK_EXPIRED_LOCK_RECLAIMER_BATCH_SIZE` | Max locks reclaimed per sweep | `1000` (clamped to min `1`) |
| `WAYMARK_SCHEDULER_POLL_INTERVAL_MS` | Scheduler poll interval | `1000` |
| `WAYMARK_SCHEDULER_BATCH_SIZE` | Scheduler due-item batch size | `100` |
| `WAYMARK_RUNNER_PROFILE_INTERVAL_MS` | Worker status/profile publish interval | `5000` (clamped to min `1`) |

If you need to customize Python startup/bootstrap behavior (for example custom boot commands), see `Bootstrap / Python SDK overrides` below.

### `waymark-bridge` runtime

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `WAYMARK_BRIDGE_GRPC_ADDR` | gRPC bind address for bridge server | `127.0.0.1:24117` |
| `WAYMARK_BRIDGE_IN_MEMORY` | Enables in-memory mode (no Postgres backend) | `false` |
| `WAYMARK_DATABASE_URL` | PostgreSQL DSN (required unless in-memory mode) | required unless `WAYMARK_BRIDGE_IN_MEMORY` is truthy |

### Bootstrap / Python SDK overrides

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `WAYMARK_BOOT_COMMAND` | Full command used by Python SDK to boot singleton bridge | unset |
| `WAYMARK_BOOT_BINARY` | Boot binary used when `WAYMARK_BOOT_COMMAND` is unset | `boot-waymark-singleton` |
| `WAYMARK_BRIDGE_GRPC_ADDR` | Explicit bridge gRPC target (`host:port`) for Python SDK + singleton helper | unset |
| `WAYMARK_BRIDGE_GRPC_HOST` | Bridge gRPC host used by singleton probing/boot + Python SDK | `127.0.0.1` |
| `WAYMARK_BRIDGE_GRPC_PORT` | Bridge gRPC base port used by singleton probing/boot + Python SDK | `24117` |
| `WAYMARK_BRIDGE_BASE_PORT` | Fallback alias for `WAYMARK_BRIDGE_GRPC_PORT` in singleton helper | unset |
| `WAYMARK_SKIP_WAIT_FOR_INSTANCE` | Python SDK: return immediately after queueing workflow run | `false` |
| `WAYMARK_LOG_LEVEL` | Python SDK logger level (`DEBUG`, `INFO`, etc.) | `INFO` |

### Worker Recycling

The `WAYMARK_MAX_ACTION_LIFECYCLE` setting controls how many actions a Python worker process can execute before being automatically recycled (shut down and replaced with a fresh process). This can help mitigate memory leaks in third-party libraries that may accumulate memory over time.

When a worker reaches its action limit, waymark spawns a replacement worker before retiring the old one. Any in-flight actions on the old worker will complete normally before the process terminates. This ensures zero downtime during recycling.

By default, this is set to `None` (no limit), meaning workers run indefinitely. If you notice memory growth in your workers over time, try setting this to a value like `1000` or `10000` depending on your action characteristics.

## Philosophy

Background jobs in webapps are so frequently used that they should really be a primitive of your fullstack library: database, backend, frontend, _and_ background jobs. Otherwise you're stuck in a situation where users either have to always make blocking requests to an API or you spin up ephemeral tasks that will be killed during re-deployments or an accidental docker crash.

After trying most of the ecosystem in the last 3 years, I believe background jobs should provide a few key features:

- Easy to write control flow in normal Python
- Should be both very simple to test locally and very simple to deploy remotely
- Reasonable default configurations to scale to a reasonable request volume without performance tuning

On the point of control flow, we shouldn't be forced into a DAG definition (decorators, custom syntax). It should be regular control flow just distinguished because the flows are durable and because some portions of the parallelism can be run across machines.

Nothing on the market provides this balance - `waymark` aims to try. We don't expect ourselves to reach best in class functionality for load performance. Instead we intend for this to scale _most_ applications well past product market fit.

## How It Works

Waymark takes a different approach from replay-based workflow engines like Temporal or Vercel Workflow.

| Approach | How it works | Constraint on users |
|----------|-------------|-------------------|
| **Temporal/Vercel Workflows** | Replay-based. Your workflow code re-executes from the beginning on each step; completed activities return cached results. | Code must be deterministic. No `random()`, no `datetime.now()`, no side effects in workflow logic. |
| **Waymark** | Compile-once. Parse your Python AST → intermediate representation → DAG. Execute the DAG directly. Your code never re-runs. | Code must use supported patterns. But once parsed, a node is self-aware where it lives in the computation graph. |

When you decorate a class with `@workflow`, Waymark parses the `run()` method's AST and compiles it to an intermediate representation (IR). This IR captures your control flow—loops, conditionals, parallel branches—as a static directed graph. The DAG is stored in Postgres and executed by the Rust runtime. Your original Python run definition is never re-executed during workflow recovery.

This is convenient in practice because it means that if your workflow compiles, your workflow will run as advertised. There's no need to hack around stdlib functions that are non-deterministic (like time/uuid/etc) because you'll get an error on compilation to switch these into an explicit `@action` where all non-determinism should live.

## Other options

**When should you use Waymark?**

- You're already using Python & Postgres for the core of your stack, either with Mountaineer or FastAPI
- You have a lot of async heavy logic that needs to be durable and can be retried if it fails (common with 3rd party API calls, db jobs, etc)
- You want something that works the same locally as when deployed remotely
- You want background job code to plug and play with your existing unit test & static analysis stack
- You are focused on getting to product market fit versus scale

Performance is a top priority of waymark. That's why it's written with a Rust core, is lightweight on your database connection by isolating them to ~1 pool per machine host, and runs continuous benchmarks on CI. But it's not the _only_ priority. After all there's only so much we can do with Postgres as an ACID backing store. Once you start to tax Postgres' capabilities you're probably at the scale where you should switch to a more complicated architecture.

**When shouldn't you?**

- You have particularly latency sensitive background jobs, where you need <100ms acknowledgement and handling of each task.
- You have a huge scale of concurrent background jobs, order of magnitude >10k actions being coordinated concurrently.
- You have tried some existing task coordinators and need to scale your solution to the next 10x worth of traffic.

There is no shortage of robust background queues in Python, including ones like Temporal.io/RabbitMQ that scale to millions of requests a second.

Almost all of these require a dedicated task broker that you host alongside your app. This usually isn't a huge deal during POCs but can get complex as you need to performance tune it for production. Cloud hosting of most of these are billed per-event and can get very expensive depending on how you orchestrate your jobs. They also typically force you to migrate your logic to fit the conventions of the framework.

Open source solutions like RabbitMQ have been battle tested over decades & large companies like Temporal are able to throw a lot of resources towards optimization. Both of these solutions are great choices - just intended to solve for different scopes. Expect an associated higher amount of setup and management complexity.

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
```

Add `--json` to the parser if you prefer JSON output.
