# waymark

![Waymark Logo](https://raw.githubusercontent.com/piercefreeman/waymark/main/media/header.png)

waymark is a library to let you build durable background tasks that withstand server restarts, task crashes, and long-running jobs. It's built for Python and Postgres without any additional deploy time requirements. More languages are coming soon.

**Documentation: [waymark.sh](https://waymark.sh)**

Getting started:

- [Quickstart with Python](https://waymark.sh/python/quickstart) - install, write a workflow, run it
- [Why Waymark](https://waymark.sh/guides/motivation) - the motivation and the workloads it's built for

Running Waymark:

- [Configuration](https://waymark.sh/guides/configuration) - every environment variable
- [Webapp](https://waymark.sh/guides/webapp) - the built-in view of your instances and nodes
- [Production Deployment](https://waymark.sh/guides/production) - images, services, connections, scaling

Python:

- [Workflows & Actions](https://waymark.sh/python/workflows-and-actions) - the two primitives
- [Control Flow](https://waymark.sh/python/control-flow) - what a workflow body can contain, and its [known issues](https://waymark.sh/python/control-flow#known-issues)
- [Retries & Timeouts](https://waymark.sh/python/retries) - per-call retry policies and timeouts
- [Scheduled Workflows](https://waymark.sh/python/scheduling) - cron and interval schedules

## Usage with Python

We ship all client and server binaries in one Python package. Install it via your package manager of choice:

```bash
uv add waymark
```

Let's say you need to send welcome emails to a batch of users, but only the active ones. You want to fetch them all, filter out inactive accounts, then fan out emails in parallel. Actions are the distributed work - plain async functions, sent to your workers:

```python
from typing import Annotated

from waymark import Depends, action

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

The workflow is the durable control flow that orchestrates them:

```python
import asyncio

from waymark import Workflow, workflow

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

Run the node - the process that executes workflows and their actions - against your database:

```bash
export WAYMARK_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/waymark
export WAYMARK_HTTP_ENABLED=true  # the webapp, on http://localhost:24119
uv run waymark-start-workers
```

Then kick off the workflow from any Python process, and wait for its result:

```python
async def welcome_users(user_ids: list[str]):
    await WelcomeEmailWorkflow().run(user_ids)
```

None of this executes inline in your webserver: the run is queued to Postgres and executed by the node.

**Actions** are the distributed work: network calls, database queries, anything that can fail and should be retried independently.

**Workflows** are the control flow: loops, conditionals, parallel branches. They orchestrate actions but don't do heavy lifting themselves.

## What you can write

Workflows are plain async Python. A few of the things they can do:

1. **Retries and timeouts, per call.** By default an action runs once, like a regular function call. Wrap a call in `self.run_action(...)` to give it a retry policy and a timeout - see [Retries & Timeouts](https://waymark.sh/python/retries).

    ```python
    from datetime import timedelta

    from waymark import RetryPolicy

    async def run(self, order_id: str) -> Receipt:
        return await self.run_action(
            charge_card(order_id),
            retry=RetryPolicy(attempts=5, backoff_seconds=10),
            timeout=timedelta(minutes=2),
        )
    ```

1. **Branches and loops.** `if`/`elif`/`else`, `for` and `while` compile into the workflow program and are executed by the runtime, just like your actions.

    ```python
    async def run(self, order_ids: list[str]) -> list[str]:
        shipped = []
        for order_id in order_ids:
            status = await fetch_status(order_id)
            if status == "shipped":
                shipped.append(order_id)
        return shipped
    ```

1. **Parallel fan-out and durable sleep.** `asyncio.gather` runs actions in parallel; `asyncio.sleep` pauses the workflow durably, surviving restarts - for a second or for a day.

    ```python
    import asyncio

    async def run(self, user_id: str) -> str:
        profile, history = await asyncio.gather(
            fetch_profile(user_id),
            fetch_purchase_history(user_id),
            return_exceptions=True,
        )

        # wait a day before following up
        await asyncio.sleep(24 * 60 * 60)
        return await send_recommendations(profile, history)
    ```

1. **Schedules.** Run a workflow on a cron expression or a fixed interval, with no extra infrastructure - see [Scheduled Workflows](https://waymark.sh/python/scheduling).

    ```python
    await schedule_workflow(DataSyncWorkflow, schedule_name="hourly", schedule="0 * * * *")
    ```

1. **A built-in webapp.** `waymark-start-workers` serves a [webapp](https://waymark.sh/guides/webapp) showing every workflow instance, each one's timeline of calls, and the load on your nodes.

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
| --- | --- | --- |
| **Temporal/Vercel Workflows** | Replay-based. Your workflow code re-executes from the beginning on each step; completed activities return cached results. | Code must be deterministic. No `random()`, no `datetime.now()`, no side effects in workflow logic. |
| **Waymark** | Compile-once. Parse your Python AST → intermediate representation → bytecode. A durable VM executes the bytecode. Your code never re-runs. | Code must use supported patterns. But once compiled, the runtime always knows exactly where the workflow is in its execution. |

The first time a workflow is run or scheduled, the Python SDK parses its `run()` method's AST and compiles it to an intermediate representation (IR). This IR captures your control flow - loops, conditionals, parallel branches - and is lowered to bytecode for a durable virtual machine. The Rust runtime executes the bytecode and snapshots the VM's state to Postgres as it goes, so a workflow resumes from where it was after a crash. Your original Python `run()` definition is never re-executed during workflow recovery.

This is convenient in practice because it means that if your workflow compiles, your workflow will run as advertised. There's no need to hack around stdlib functions that are non-deterministic (like time/uuid/etc) because you'll get an error on compilation to switch these into an explicit `@action`. The few patterns that currently slip past the compiler are listed under [known issues](https://waymark.sh/python/control-flow#known-issues) and tracked in [#772](https://github.com/piercefreeman/waymark/issues/772).

## When to use it

**When should you use Waymark?**

- You're already using Python & Postgres for the core of your stack, either with Mountaineer or FastAPI
- You have a lot of async heavy logic that needs to be durable and can be retried if it fails (common with 3rd party API calls, db jobs, etc)
- You want something that works the same locally as when deployed remotely
- You want background job code to plug and play with your existing unit test & static analysis stack
- You are focused on getting to product market fit versus scale

Performance is a top priority of waymark. That's why it's written with a Rust core and runs continuous benchmarks on CI. But it's not the _only_ priority. After all there's only so much we can do with Postgres as an ACID backing store. Once you start to tax Postgres' capabilities you're probably at the scale where you should switch to a more complicated architecture.

**When shouldn't you?**

- You have particularly latency sensitive background jobs, where you need <100ms acknowledgement and handling of each task.
- You have a huge scale of concurrent background jobs, order of magnitude >10k actions being coordinated concurrently.
- You have tried some existing task coordinators and need to scale your solution to the next 10x worth of traffic.

There is no shortage of robust background queues in Python, including ones like Temporal.io/RabbitMQ that scale to millions of requests a second.

Almost all of these require a dedicated task broker that you host alongside your app. This usually isn't a huge deal during POCs but can get complex as you need to performance tune it for production. Cloud hosting of most of these are billed per-event and can get very expensive depending on how you orchestrate your jobs. They also typically force you to migrate your logic to fit the conventions of the framework.

Open source solutions like RabbitMQ have been battle tested over decades & large companies like Temporal are able to throw a lot of resources towards optimization. Both of these solutions are great choices - just intended to solve for different scopes. Expect an associated higher amount of setup and management complexity.

## Project Status

> [!IMPORTANT]
> Right now you shouldn't use waymark in any production applications. The spec is changing too quickly and we don't guarantee backwards compatibility before 1.0.0. But we would love if you try it out in your side project and see how you find it.

If you have a particular workflow that you think should be working but isn't yet compiling correctly, please file an issue.

## Building from source

The webapp is compiled into `waymark-start-workers`, so building from source requires Node.js (version in `.node-version`) and npm:

```sh
make js-deps
cargo build --bin waymark-start-workers
```

## Contributing

If you want to contribute, check out the [contributing guidelines](./CONTRIBUTING.md).
