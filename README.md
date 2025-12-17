# rappel

![Rappel Logo](https://raw.githubusercontent.com/piercefreeman/rappel/main/media/header.png)

rappel is a library to let you build durable background tasks that withstand server restarts, task crashes, and long-running jobs. It's built for Python and Postgres without any additional deploy time requirements.

## Usage

Let's say you need to send welcome emails to a batch of users, but only the active ones. You want to fetch them all, filter out inactive accounts, then fan out emails in parallel. This is how you write that workflow in rappel:

```python
import asyncio
from rappel import Workflow, action, workflow

@workflow
class WelcomeEmailWorkflow(Workflow):
    async def run(self, user_ids: list[str]) -> list[EmailResult]:
        users = await fetch_users(user_ids)
        active_users = [user for user in users if user.active]

        results = await asyncio.gather(*[
            send_email(to=user.email, subject="Welcome")
            for user in active_users
        ])
        
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

When you call `await workflow.run()`, we parse the AST of your `run()` method and compile it into the Rappel Runtime Language. The `for` loop becomes a filter node, the `asyncio.gather` becomes a parallel fan-out. None of this executes inline in your webserver, instead it's queued to Postgres and orchestrated by the Rust runtime across your worker cluster.

**Actions** are the distributed work: network calls, database queries, anything that can fail and should be retried independently.

**Workflows** are the control flow: loops, conditionals, parallel branches. They orchestrate actions but don't do heavy lifting themselves.

### Complex Workflows

Workflows can get much more complex than the example above:

1. Customizable retry policy

    By default your Python code will execute like native logic would: any exceptions will throw and immediately fail. Actions are set to timeout after ~5min to keep the queues from backing up - although we will continuously retry timed out actions in case they were caused by a failed node in your cluster. If you want to control this logic to be more robust, you can set retry policies and backoff intervals so you can attempt the action multiple times until it succeeds.

    ```python
    from rappel import RetryPolicy, BackoffPolicy
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
            fetch_purchase_history(user_id=user_id)
        ) 

        # wait before sending email
        await asyncio.sleep(24*60*60)
        recommendations = await email_ping(history)

        return Summary(profile=profile, settings=settings, recommendations=recommendations)
    ```

### Error handling

To build truly robust background tasks, you need to consider how things can go wrong. Actions can 'fail' in a couple ways. This is supported by our `.run_action` syntax that allows users to provide additional parameters to modify the execution bounds on each action.

1. Action explicitly throws an error and we want to retry it. Caused by intermittent database connectivity / overloaded webservers / or simply buggy code will throw an error. This comes from a standard python `raise Exception()`
1. Actions raise an error that is a really a RappelTimeout. This indicates that we dequeued the task but weren't able to complete it in the time allocated. This could be because we dequeued the task, started work on it, then the server crashed. Or it could still be running in the background but simply took too much time. Either way we will raise a synthetic error that is representative of this execution.

By default we will only try explicit actions one time if there is an explicit exception raised. We will try them infinite times in the case of a timeout since this is usually caused by cross device coordination issues.

## Project Status

_NOTE: Right now you shouldn't use rappel in any production applications. The spec is changing too quickly and we don't guarantee backwards compatibility before 1.0.0. But we would love if you try it out in your side project and see how you find it._

Rappel is in an early alpha. Particular areas of focus include:

1. Finalizing the Rappel Runtime Language
1. Extending AST parsing logic to handle most core control flows
1. Performance tuning
1. Unit and integration tests

If you have a particular workflow that you think should be working but isn't yet producing the correct DAG (you can visualize it via CLI by `.visualize()`) please file an issue.

## Configuration

The main rappel configuration is done through env vars, which is what you'll typically use in production when using a docker deployment pipeline. If we can't find an environment parameter we will fallback to looking for an .env that specifies it within your local filesystem.

These are the primary environment parameters that you'll likely want to customize for your deployment:

| Environment Variable | Description | Default | Example |
|---------------------|-------------|---------|---------|
| `RAPPEL_DATABASE_URL` | PostgreSQL connection string for the rappel server | (required on bridge &workers ) | `postgresql://user:pass@localhost:5433/rappel` |
| `RAPPEL_WORKER_COUNT` | Number of Python worker processes | `num_cpus` | `8` |
| `RAPPEL_CONCURRENT_PER_WORKER` | Max concurrent actions per worker | `10` | `20` |
| `RAPPEL_USER_MODULE` | Python module preloaded into each worker | none | `my_app.actions` |
| `RAPPEL_POLL_INTERVAL_MS` | Poll interval for the dispatch loop (ms) | `100` | `50` |
| `RAPPEL_WEBAPP_ENABLED` | Enable the web dashboard | `false` | `true` |
| `RAPPEL_WEBAPP_ADDR` | Web dashboard bind address | `0.0.0.0:24119` | `0.0.0.0:8080` |

We expect that you won't need to modify the following env parameters, but we provide them for convenience:

| Environment Variable | Description | Default | Example |
|---------------------|-------------|---------|---------|
| `RAPPEL_HTTP_ADDR` | HTTP bind address for `rappel-bridge` | `127.0.0.1:24117` | `0.0.0.0:24117` |
| `RAPPEL_GRPC_ADDR` | gRPC bind address for `rappel-bridge` | HTTP port + 1 | `0.0.0.0:24118` |
| `RAPPEL_BATCH_SIZE` | Max actions fetched per poll | `workers * concurrent_per_worker` | `200` |

## Philosophy

Background jobs in webapps are so frequently used that they should really be a primitive of your fullstack library: database, backend, frontend, _and_ background jobs. Otherwise you're stuck in a situation where users either have to always make blocking requests to an API or you spin up ephemeral tasks that will be killed during re-deployments or an accidental docker crash.

After trying most of the ecosystem in the last 3 years, I believe background jobs should provide a few key features:

- Easy to write control flow in normal Python
- Should be both very simple to test locally and very simple to deploy remotely
- Reasonable default configurations to scale to a reasonable request volume without performance tuning

On the point of control flow, we shouldn't be forced into a DAG definition (decorators, custom syntax). It should be regular control flow just distinguished because the flows are durable and because some portions of the parallelism can be run across machines.

Nothing on the market provides this balance - `rappel` aims to try. We don't expect ourselves to reach best in class functionality for load performance. Instead we intend for this to scale _most_ applications well past product market fit.

## How It Works

Rappel takes a different approach from replay-based workflow engines like Temporal or Vercel Workflow.

| Approach | How it works | Constraint on users |
|----------|-------------|-------------------|
| **Temporal/Vercel Workflows** | Replay-based. Your workflow code re-executes from the beginning on each step; completed activities return cached results. | Code must be deterministic. No `random()`, no `datetime.now()`, no side effects in workflow logic. |
| **Rappel** | Compile-once. Parse your Python AST → intermediate representation → DAG. Execute the DAG directly. Your code never re-runs. | Code must use supported patterns. But once parsed, a node is self-aware where it lives in the computation graph. |

When you decorate a class with `@workflow`, Rappel parses the `run()` method's AST and compiles it to an intermediate representation (IR). This IR captures your control flow—loops, conditionals, parallel branches—as a static directed graph. The DAG is stored in Postgres and executed by the Rust runtime. Your original Python run definition is never re-executed during workflow recovery.

This is convenient in practice because it means that if your workflow compiles, your workflow will run as advertised. There's no need to hack around stdlib functions that are non-deterministic (like time/uuid/etc) because you'll get an error on compilation to switch these into an explicit `@action` where all non-determinism should live.

## Other options

**When should you use Rappel?**

- You're already using Python & Postgres for the core of your stack, either with Mountaineer or FastAPI
- You have a lot of async heavy logic that needs to be durable and can be retried if it fails (common with 3rd party API calls, db jobs, etc)
- You want something that works the same locally as when deployed remotely
- You want background job code to plug and play with your existing unit test & static analysis stack
- You are focused on getting to product market fit versus scale

Performance is a top priority of rappel. That's why it's written with a Rust core, is lightweight on your database connection by isolating them to ~1 pool per machine host, and runs continuous benchmarks on CI. But it's not the _only_ priority. After all there's only so much we can do with Postgres as an ACID backing store. Once you start to tax Postgres' capabilities you're probably at the scale where you should switch to a more complicated architecture.

**When shouldn't you?**

- You have particularly latency sensitive background jobs, where you need <100ms acknowledgement and handling of each task.
- You have a huge scale of concurrent background jobs, order of magnitude >10k actions being coordinated concurrently.
- You have tried some existing task coordinators and need to scale your solution to the next 10x worth of traffic.

There is no shortage of robust background queues in Python, including ones like Temporal.io/RabbitMQ that scale to millions of requests a second.

Almost all of these require a dedicated task broker that you host alongside your app. This usually isn't a huge deal during POCs but can get complex as you need to performance tune it for production. Cloud hosting of most of these are billed per-event and can get very expensive depending on how you orchestrate your jobs. They also typically force you to migrate your logic to fit the conventions of the framework.

Open source solutions like RabbitMQ have been battle tested over decades & large companies like Temporal are able to throw a lot of resources towards optimization. Both of these solutions are great choices - just intended to solve for different scopes. Expect an associated higher amount of setup and management complexity.

## Worker Pool

`start-workers` is the main invocation point to boot your worker cluster on a new node. It launches the gRPC bridge plus a polling dispatcher that streams
queued actions from Postgres into the Python workers. You should use this as your docker entrypoint:

```bash
$ cargo run --bin start-workers
```

## Development

### Packaging

Use the helper script to produce distributable wheels that bundle the Rust executables with the
Python package:

```bash
$ uv run scripts/build_wheel.py --out-dir target/wheels
```

The script compiles every Rust binary (release profile), stages the required entrypoints
(`rappel-bridge`, `boot-rappel-singleton`) inside the Python package, and invokes
`uv build --wheel` to produce an artifact suitable for publishing to PyPI.

### Local Server Runtime

The Rust runtime exposes both HTTP and gRPC APIs via the `rappel-bridge` binary:

```bash
$ cargo run --bin rappel-bridge
```

Developers can either launch it directly or rely on the `boot-rappel-singleton` helper which finds (or starts) a single shared instance on
`127.0.0.1:24117`. The helper prints the active HTTP port to stdout so Python clients can connect without additional
configuration:

```bash
$ cargo run --bin boot-rappel-singleton
24117
```

The Python bridge automatically shells out to the helper unless you provide `RAPPEL_SERVER_URL`
(`RAPPEL_GRPC_ADDR` for direct sockets) overrides. Once the ports are known it opens a gRPC channel to the
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

The `bench` binary seeds raw actions to measure dequeue/execute/ack throughput. Use `bench_instances` for an end-to-end workflow run (queueing and executing full workflow instances via the scheduler) without installing a separate `rappel-worker` binary—the harness shells out to `uv run python -m rappel.worker` automatically:

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
