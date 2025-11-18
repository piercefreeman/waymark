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

Actions are the distributed work that your system does: these are the parallelism primitives that can be retired, throw errors independently, etc.

Instances are your control flow - also written in Python - that orchestrate the actions. They are intended to be fast business logic: list iterations. Not long-running or blocking network jobs, for instance.

### Complex Workflows

Workflows can get much more complex than the example above:

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

1. Nested logics

Use if statements, for loops, or any other Python primitives within the control logic. We will automatically detect these branches and compile them into a DAG node that gets executed just like your other actions.

```python
async def run(self) -> Summary:
  # loop + non-action helper call
  top_spenders: List[float] = []
  for record in summary.transactions.records:
      if _is_high_value(record):
          top_spenders.append(record.amount)
```

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
$ cargo run --bin boot-carabiner-singleton
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
  --database-url postgres://mountaineer:mountaineer@localhost:5433/mountaineer_daemons \
  --partition 0 | \
  uv run python/tools/parse_bench_logs.py
```

Add `--json` to the parser if you prefer JSON output.
