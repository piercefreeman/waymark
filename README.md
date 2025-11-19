# carabiner

carabiner is a library to let you build durable background tasks that withstand device restarts, task crashes, and long-running jobs. It's built for Python and Postgres without any additional deploy time requirements.

## Usage

An example is worth a thousand words:

```python
from dataclasses import dataclass
from typing import List
from myapp.models import User, GreetingSummary


class GreetingWorkflow(Workflow):
    def __init__(self, user_id: str):
        self.user_id = user_id

    async def run(self) -> GreetingSummary:
        user = await fetch_user(self.user_id)      # first action
        summary = await build_greetings(user)      # second action, chained
        return summary


@action
async def fetch_user(user_id: str) -> User:
    ...  # e.g. load from database


@action
async def build_greetings(user: User) -> GreetingSummary:
    messages: List[str] = []
    for topic in user.interests:        # loop + dot syntax on action result
        messages.append(f"Hi {user.name}, let's talk about {topic}!")
    return GreetingSummary(user=user, messages=messages)
```

Your webserver wants to greet some user but do it (1) asynchronously and (2) guarantees this happens even if your webapp crashes. When you call `await workflow.run()` from within your code we'll queue up this work in Postgres; none of the workflow logic is actually executed inline within your webserver. Instead we parse the AST definition to determine your control flow: we'll identify that `fetch_user` and `build_greetings` are decorated with `@action` and depend on the outputs of the another. We will call them in sequence, passing the data as necessary, on whatever background machines are able to handle more work. When the `summary` is returned to your original webapp caller it looks like everything just happened right in the same process. Whereas the actual code was orchestrated across multiple different machines.

Actions are the distributed work that your system does: these are the parallelism primitives that can be retired, throw errors independently, etc.

Instances are your control flow - also written in Python - that orchestrate the actions. They are intended to be fast business logic: list iterations. Not long-running or blocking network jobs, for instance.

### Complex Workflows

Workflows can get much more complex than the example above:

1. Customizable retry policy

    By default your Python code will execute like native logic would: any exceptions will throw and immediately fail. Actions are set to timeout after ~5min to keep the queues from backing up. If you want to control this logic to be more robust, you can set retry policies and backoff intervals so you can attempt the action multiple times until it succeeds.

    ```python
    from carabiner import RetryPolicy, BackoffPolicy
    from datetime import timedelta

    async def run(self):
      await self.run_action(
        inconsistent_action(0.5),
        retry=RetryPolicy(attempts=50),
        backoff=BackoffPolicy(base_delay=5),
        timeout=timedelta(minutes=10)
      )
    ```

1. Branching control flows

    Use if statements, for loops, or any other Python primitives within the control logic. We will automatically detect these branches and compile them into a DAG node that gets executed just like your other actions.

    ```python
    async def run(self) -> Summary:
      # loop + non-action helper call
      top_spenders: list[float] = []
      for record in summary.transactions.records:
          if _is_high_value(record):
              top_spenders.append(record.amount)
    ```

1. asyncio primitives

    Use asyncio.gather to parallelize tasks. Use asyncio.sleep to sleep for a longer period of time.

    ```python
    from asyncio import gather, sleep

    async def run(self) -> Summary:
        # parallelize independent actions with gather
        profile, settings, history = await gather(
            fetch_profile(user_id=self.user_id),
            fetch_settings(user_id=self.user_id),
            fetch_purchase_history(user_id=self.user_id)
        )
        
        # wait before sending email
        await sleep(24*60*60)
        recommendations = await email_ping(history)
        
        return Summary(profile=profile, settings=settings, recommendations=recommendations)
    ```

1. Helper functions

    You can declare helper functions in your file, in your class, or import helper functions from elsewhere in your project.

    ```python
    from myapp.helpers import _format_currency

    async def run(self) -> Summary:
        # actions related to one another
        profile = await fetch_profile(user_id=self.user_id)
        txns = await load_transactions(user_id=self.user_id)
        summary = await compute_summary(profile=profile, txns=txns)

        # helper functions
        pretty = _format_currency(summary.transactions.total)
    ```

## Configuration

The main carabiner configuration is done through env vars, which is what you'll typically use in production when using a docker deployment pipeline. If we can't find an environment parameter we will fallback to looking for an .env that specifies it within your local filesystem.

| Environment Variable | Description | Example |
|---------------------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string for the carabiner server | `postgresql://mountaineer:mountaineer@localhost:5433/mountaineer_daemons` |
| `CARABINER_HTTP_ADDR` | Optional HTTP bind address for `carabiner-server` | `0.0.0.0:24117` |
| `CARABINER_GRPC_ADDR` | Optional gRPC bind address for `carabiner-server` | `0.0.0.0:24118` |
| `CARABINER_WORKER_COUNT` | Override number of Python workers spawned by `start_workers` | `8` |
| `CARABINER_USER_MODULE` | Python module preloaded into each worker process | `my_app.actions` |
| `CARABINER_POLL_INTERVAL_MS` | Poll interval for the dispatch loop (ms) | `100` |
| `CARABINER_BATCH_SIZE` | Max actions fetched per poll | `100` |

## Philosophy

Background jobs in webapps are so frequently used that they should really be a primitive of your fullstack library: database, backend, frontend, _and_ background jobs. Otherwise you're stuck in a situation where users either have to always make blocking requests to an API or you spin up ephemeral tasks that will be killed during re-deployments or an accidental docker crash.

After trying most of the ecosystem in the last 3 years, I believe background jobs should provide a few key features:

- Easy to write control flow in normal Python
- Should be both very simple to test locally and very simple to deploy remotely
- Reasonable default configurations to scale to a reasonable request volume without performance tuning

On the point of control flow, we shouldn't be forced into a DAG definition (decorators, custom syntax). It should be regular control flow just distinguished because the flows are durable and because some portions of the parallelism can be run across machines.

Nothing on the market provides this balance - `carabiner` aims to try. We don't expect ourselves to reach best in class functionality for load performance. Instead we intend for this to scale _most_ applications well past product market fit.

## Other options

There is no shortage of robust background queues in Python, including ones that scale to millions of requests a second:

1. Temporal.io
2. Celery/RabbitMQ

Almost all of these require a dedicated task broker that you host alongside your app. This usually isn't a huge deal during POCs, but they all have a ton of knobs and dials so you can performance tune it to your own environment. It's also yet another thing in which to build a competency. Cloud hosting of most of these are billed per-event and can get very expensive depending on how you orchestrate your jobs.

## Local Server Runtime

The Rust runtime exposes both HTTP and gRPC APIs via the `carabiner-server` binary:

```bash
$ cargo run --bin carabiner-server
```

Developers can either launch it directly or rely on the `boot-carabiner-singleton` helper which finds (or starts) a single shared instance on
`127.0.0.1:24117`. The helper prints the active HTTP port to stdout so Python clients can connect without additional
configuration:

```bash
$ cargo run --bin boot-carabiner-singleton
24117
```

The Python bridge automatically shells out to the helper unless you provide `CARABINER_SERVER_URL`
(`CARABINER_GRPC_ADDR` for direct sockets) overrides. Once the ports are known it opens a gRPC channel to the
`WorkflowService`.

## Packaging

Use the helper script to produce distributable wheels that bundle the Rust executables with the
Python package:

```bash
$ uv run scripts/build_wheel.py --out-dir target/wheels
```

The script compiles every Rust binary (release profile), stages the required entrypoints
(`carabiner-server`, `boot-carabiner-singleton`) inside the Python package, and invokes
`uv build --wheel` to produce an artifact suitable for publishing to PyPI.

## Benchmarking

Stream benchmark output directly into our parser to summarize throughput and latency samples:

```bash
$ cargo run --bin bench -- \
  --messages 100000 \
  --payload 1024 \
  --concurrency 64 \
  --workers 4 \
  --log-interval 15 \
  uv run python/tools/parse_bench_logs.py
```

Add `--json` to the parser if you prefer JSON output.

## Worker Pool Runtime

`start_workers` launches the gRPC bridge plus a polling dispatcher that streams
queued actions from Postgres into the Python workers:

```bash
$ cargo run --bin start_workers
```
