# rappel

![Rappel Logo](https://raw.githubusercontent.com/piercefreeman/rappel/main/media/header.png)

rappel is a library to let you build durable background tasks that withstand device restarts, task crashes, and long-running jobs. It's built for Python and Postgres without any additional deploy time requirements.

## Usage

An example is worth a thousand words. Here's how you define your workflow:

```python
from rappel import Workflow, action
from myapp.models import User, GreetingSummary
from myapp.db import my_db

class GreetingWorkflow(Workflow):
    async def run(self, user_id: str):
        user = await fetch_user(user_id)      # first action
        summary = await build_greetings(user)      # second action, chained
        return summary
```

And here's how you describe your distributed actions:

```python
@action
async def fetch_user(user_id: str) -> User:
    return await my_db.get(User, user_id)


@action
async def build_greetings(user: User) -> GreetingSummary:
    messages: List[str] = []
    for topic in user.interests:
        messages.append(f"Hi {user.name}, let's talk about {topic}!")
    return GreetingSummary(user=user, messages=messages)
```

Your webserver wants to greet some user but do it (1) asynchronously and (2) guarantee this happens even if your webapp crashes. When you call `await workflow.run()` from within your code we'll queue up this work in Postgres; none of the workflow logic is actually executed inline within your webserver. We start by parsing the AST definition to determine your control flow and identify that `fetch_user` and `build_greetings` are decorated with `@action` and depend on the outputs of the another. We will call them in sequence, passing the data as necessary, on whatever background machines are able to handle more work. When the `summary` is returned to your original webapp caller it looks like everything just happened right in the same process. Whereas the actual code was orchestrated across multiple different machines.

Actions are the distributed work that your system does: these are the parallelism primitives that can be retired, throw errors independently, etc.

Instances are your control flow - also written in Python - that orchestrate the actions. They are intended to be fast business logic: list iterations. Not long-running or blocking network jobs, for instance.

### Complex Workflows

Workflows can get much more complex than the example above:

1. Customizable retry policy

    By default your Python code will execute like native logic would: any exceptions will throw and immediately fail. Actions are set to timeout after ~5min to keep the queues from backing up. If you want to control this logic to be more robust, you can set retry policies and backoff intervals so you can attempt the action multiple times until it succeeds.

    ```python
    from rappel import RetryPolicy, BackoffPolicy
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
    async def run(self, user_id: str) -> Summary:
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

    async def run(self, user_id: str) -> Summary:
        # parallelize independent actions with gather
        profile, settings, history = await gather(
            fetch_profile(user_id=user_id),
            fetch_settings(user_id=user_id),
            fetch_purchase_history(user_id=user_id)
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

    async def run(self, user_id: str) -> Summary:
        # actions related to one another
        profile = await fetch_profile(user_id=user_id)
        txns = await load_transactions(user_id=user_id)
        summary = await compute_summary(profile=profile, txns=txns)

        # helper functions
        pretty = _format_currency(summary.transactions.total)
    ```

### Error handling

To build truly robust background tasks, you need to consider how things can go wrong. Actions can 'fail' in a few ways. This is supported by our `.run_action` syntax that allows users to provide additional parameters to modify the execution bounds on each action.

1. Action explicitly throws an error and we want to retry it. Caused by intermittent database connectivity / overloaded webservers / or simply buggy code will throw an error.
1. Actions raise an error that is a really a RappelTimeout. This indicates that we dequeued the task but weren't able to complete it in the time allocated. This could be because we dequeued the task, started work on it, then the server crashed. Or it could still be running in the background but simply took too much time. Either way we will raise a synthetic error that is representative of this execution.

By default we will only try explicit actions one time if there is an explicit exception raised. We will try them infinite times in the case of a timeout since this is usually caused by cross device coordination issues.

## Project Status

Rappel is in an early alpha. Particular areas of focus include:

1. Extending AST parsing logic to handle most core control flows
1. Performance tuning
1. Unit and integration tests

If you have a particular workflow that you think should be working but isn't yet producing the correct DAG (you can visualize it via CLI by `.visualize()`) please file an issue.

## Configuration

The main rappel configuration is done through env vars, which is what you'll typically use in production when using a docker deployment pipeline. If we can't find an environment parameter we will fallback to looking for an .env that specifies it within your local filesystem.

| Environment Variable | Description | Example |
|---------------------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string for the rappel server | `postgresql://mountaineer:mountaineer@localhost:5433/mountaineer_daemons` |
| `CARABINER_HTTP_ADDR` | Optional HTTP bind address for `rappel-server` | `0.0.0.0:24117` |
| `CARABINER_GRPC_ADDR` | Optional gRPC bind address for `rappel-server` | `0.0.0.0:24118` |
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

Nothing on the market provides this balance - `rappel` aims to try. We don't expect ourselves to reach best in class functionality for load performance. Instead we intend for this to scale _most_ applications well past product market fit.

## Other options

**When should you use Rappel?**

- You're already using Python & Postgres for the core of your stack, either with Mountaineer or FastAPI
- You have a lot of async heavy logic that needs to be durable and can be retried if it fails (common with 3rd party API calls, db jobs, etc)
- You want something that works the same locally as when deployed remotely
- You want background job code to plug and play with your existing unit test & static analysis stack
- You are focused on getting to product market fit versus scale

Performance is a top priority of rappel. That's why it's written with a Rust core, is lightweight on your database connection by minimizing connections to ~1 per machine host, and runs continuous benchmarks on CI. But it's not the _only_ priority. After all there's only so much we can do with Postgres as an ACID backing store. Once you start to tax Postgres' capabilities you're probably at the scale where you should switch to a more complicated architecture.

**When shouldn't you?**

- You have particularly latency sensitive background jobs, where you need <100ms acknowledgement and handling of each task.
- You have a huge scale of concurrent background jobs, order of magnitude >10k actions being coordinated concurrently.
- You have tried some existing task coordinators and need to scale your solution to the next 10x worth of traffic.

There is no shortage of robust background queues in Python, including ones that scale to millions of requests a second:

1. Temporal.io
2. Celery/RabbitMQ
3. Redis

Almost all of these require a dedicated task broker that you host alongside your app. This usually isn't a huge deal during POCs but can get complex as you need to performance tune it for production. Cloud hosting of most of these are billed per-event and can get very expensive depending on how you orchestrate your jobs. They also typically force you to migrate your logic to fit the conventions of the framework.

Open source solutions like RabbitMQ have been battle tested over decades & large companies like Temporal are able to throw a lot of resources towards optimization. Both of these solutions are great choices - just intended to solve for different scopes. Expect an associated higher amount of setup and management complexity.

## Worker Pool

`start_workers` is the main invocation point to boot your worker cluster on a new node. It launches the gRPC bridge plus a polling dispatcher that streams
queued actions from Postgres into the Python workers. You should use this as your docker entrypoint:

```bash
$ cargo run --bin start_workers
```

## Development

### Packaging

Use the helper script to produce distributable wheels that bundle the Rust executables with the
Python package:

```bash
$ uv run scripts/build_wheel.py --out-dir target/wheels
```

The script compiles every Rust binary (release profile), stages the required entrypoints
(`rappel-server`, `boot-rappel-singleton`) inside the Python package, and invokes
`uv build --wheel` to produce an artifact suitable for publishing to PyPI.

### Local Server Runtime

The Rust runtime exposes both HTTP and gRPC APIs via the `rappel-server` binary:

```bash
$ cargo run --bin rappel-server
```

Developers can either launch it directly or rely on the `boot-rappel-singleton` helper which finds (or starts) a single shared instance on
`127.0.0.1:24117`. The helper prints the active HTTP port to stdout so Python clients can connect without additional
configuration:

```bash
$ cargo run --bin boot-rappel-singleton
24117
```

The Python bridge automatically shells out to the helper unless you provide `CARABINER_SERVER_URL`
(`CARABINER_GRPC_ADDR` for direct sockets) overrides. Once the ports are known it opens a gRPC channel to the
`WorkflowService`.

### Benchmarking

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
