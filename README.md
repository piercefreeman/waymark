# waymark

![Waymark Logo](https://raw.githubusercontent.com/piercefreeman/waymark/main/media/header.png)

waymark is a library to let you build durable background tasks that withstand server restarts, task crashes, and long-running jobs. It's built for Python and Postgres without any additional deploy time requirements. More languages are coming soon.

## Usage

We ship all client and server wheels as a python package. Install it via your package manager of choice:

```bash
uv add waymark
```

Once installed, Waymark exposes `waymark-start-workers` as a runnable bin entrypoint in your environment.
You can boot the worker pool directly with `uv run`:

```bash
export WAYMARK_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/waymark
uv run waymark-start-workers
```

Let's say you need to send welcome emails to a batch of users, but only the active ones. You want to fetch them all, filter out inactive accounts, then fan out emails in parallel. This is how you write that workflow in waymark:

```python
import asyncio
from waymark import Depends, Workflow, action, workflow

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
    db: Annotated[Database, Depends(get_db)],
) -> list[User]:
    return await db.get_many(User, user_ids)

@action
async def send_email(
    to: str,
    subject: str,
    emailer: Annotated[EmailClient, Depends(get_email_client)],
) -> EmailResult:
    return await emailer.send(to=to, subject=subject)
```

Waymark re-exports `mountaineer-di`'s `Depends(...)` helper directly. The older
`Depend(...)` name remains available as a compatibility alias.

To kick off a background job and wait for completion:

```python
async def welcome_users(user_ids: list[str]):
    workflow = WelcomeEmailWorkflow()
    await workflow.run(user_ids)
```

When you call `await workflow.run()`, we parse the AST of your `run()` method and compile it into the Waymark Runtime Language. The `for` loop becomes a loop in the compiled program, the `asyncio.gather` becomes a parallel fan-out. None of this executes inline in your webserver, instead it's queued to Postgres and orchestrated by the Rust runtime across your worker cluster.

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

    Use if statements, for loops, or any other Python primitives within the control logic. We will automatically detect these branches and compile them into the workflow program, so they're executed by the runtime just like your actions.

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

### Configuration

Waymark runtime configuration is environment-variable driven.
Waymark reads the process environment directly; it does not auto-load `.env` files.

### `waymark-start-workers` runtime

#### Commonly customized

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `WAYMARK_DATABASE_URL` | PostgreSQL DSN for worker runtime state/backend | required |
| `WAYMARK_DATABASE_MAX_CONNECTIONS` | Connection cap for the main database pool | `10` |
| `WAYMARK_WORKER_COUNT` | Number of Python worker processes | host CPU count (`available_parallelism`) |
| `WAYMARK_CONCURRENT_PER_WORKER` | Max concurrent actions per Python worker | `10` |
| `WAYMARK_MAX_CONCURRENT_INSTANCES` | Max workflow instances held concurrently | `500` |
| `WAYMARK_USER_MODULE` | Comma-separated Python modules preloaded in workers | unset |
| `WAYMARK_MAX_ACTION_LIFECYCLE` | Max actions per worker before worker recycle | unset (no recycle limit) |
| `WAYMARK_HTTP_ENABLED` | Enable the HTTP interface | `false` |
| `WAYMARK_HTTP_ADDR` | HTTP server bind address | `0.0.0.0:24119` |
| `WAYMARK_OBSERVABILITY_DATABASE_URL` | DSN for the observability store, backend picked by URL scheme; it gets its own schemas and pools even when sharing the main database | `WAYMARK_DATABASE_URL` |
| `WAYMARK_OBSERVABILITY_POSTGRES_MAX_CONNECTIONS` | Connection cap per observability pool (Postgres store); when sharing the main database these are additive to the main pool's connections | `4` |

#### Advanced tuning

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `WAYMARK_WORKER_GRPC_ADDR` | gRPC bind addr used by the Python worker bridge server | `127.0.0.1:24118` |
| `WAYMARK_LOCK_TTL_MS` | Workload pinning TTL | `15000` |
| `WAYMARK_LOCK_HEARTBEAT_MS` | Workload pinning heartbeat interval | `5000` |
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
| `WAYMARK_BOOT_BINARY` | Boot binary used when `WAYMARK_BOOT_COMMAND` is unset | `waymark-boot-singleton` |
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

## Project Status

> [!IMPORTANT]
> Right now you shouldn't use waymark in any production applications. The spec is changing too quickly and we don't guarantee backwards compatibility before 1.0.0. But we would love if you try it out in your side project and see how you find it.

Waymark is in an early alpha. Particular areas of focus include:

1. Finalizing the Waymark Runtime Language
1. Extending AST parsing logic to handle most core control flows
1. Performance tuning
1. Unit and integration tests

If you have a particular workflow that you think should be working but isn't yet compiling correctly, please file an issue.

## Philosophy

Background jobs in webapps are so frequently used that they should really be a primitive of your fullstack library: database, backend, frontend, _and_ background jobs. Otherwise you're stuck in a situation where users either have to always make blocking requests to an API or you spin up ephemeral tasks that will be killed during re-deployments or an accidental docker crash.

After trying most of the ecosystem in the last 3 years, I believe background jobs should provide a few key features:

- Easy to write control flow in normal Python
- Should be both very simple to test locally and very simple to deploy remotely
- Reasonable default configurations to scale to a reasonable request volume without performance tuning

On the point of control flow, we shouldn't be forced into a DAG definition (decorators, custom syntax). It should be regular control flow just distinguished because the flows are durable and because some portions of the parallelism can be run across machines.

Nothing on the market provides this balance - `waymark` aims to try. We don't expect ourselves to reach best in class functionality for load performance. Instead we intend for this to scale _most_ applications well past product market fit.

### How It Works

Waymark takes a different approach from replay-based workflow engines like Temporal or Vercel Workflow.

| Approach | How it works | Constraint on users |
|----------|-------------|-------------------|
| **Temporal/Vercel Workflows** | Replay-based. Your workflow code re-executes from the beginning on each step; completed activities return cached results. | Code must be deterministic. No `random()`, no `datetime.now()`, no side effects in workflow logic. |
| **Waymark** | Compile-once. Parse your Python AST → intermediate representation → bytecode. A durable VM executes the bytecode. Your code never re-runs. | Code must use supported patterns. But once compiled, the runtime always knows exactly where the workflow is in its execution. |

When you decorate a class with `@workflow`, Waymark parses the `run()` method's AST and compiles it to an intermediate representation (IR). This IR captures your control flow—loops, conditionals, parallel branches—and is lowered to bytecode for a durable virtual machine. The bytecode is stored in Postgres and executed by the Rust runtime, which snapshots VM state as it goes. Your original Python run definition is never re-executed during workflow recovery.

This is convenient in practice because it means that if your workflow compiles, your workflow will run as advertised. There's no need to hack around stdlib functions that are non-deterministic (like time/uuid/etc) because you'll get an error on compilation to switch these into an explicit `@action` where all non-determinism should live.

### Other options

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

## Contributing

If you want to contribute, check out the [contributing guidelines](./CONTRIBUTING.md).
