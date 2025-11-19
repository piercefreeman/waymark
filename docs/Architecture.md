# Architecture

Within our workers, every bit of Python logic that's run is technically an action. These can be explicit actions which are defined via an `@action` decorator or these can be implicit actions which are the bits of `Workflow.run` control flow that actually require a python interpreter to run.

Python Client Lib -> Rust Client Bridge -> DB
DB -> Rust Workflow Runner -> Python action runners

This is the semantic flow of how we expect things to work and be scaled. The implementation is a bit different: clients are expected to install a single Python library (wheel bundled for machine code) that will give them both the declarative syntax to define workflows/actions and the CLI entrypoint to be able to kick off action workers.

_Let's say you are trying to queue up a new workflow and have it run_

When clients interact with carabiner-py (client), we will auto-boot a system-wide carabiner-rs (server) singleton that's bound on a known port. grpc is our communication protocol between client languages and our core code. For every Workflow implementation decorated by `@workflow`, carabiner-py will parse the AST of the run() implementation and convert it into our custom DAG format. Python is in charge of determining where to break the business logic to create both explicit and implicit actions. It's in a better place to do this with rust because it can actually perform runtime-introspection of the dependent modules and functions without having to resort to static analysis. Python will send these DAG definitions to the server.

The server will receive the DAG definitions. It will then upsert these definitions into the database. If the logic has changed we will automatically create a new version of the workflow instance - users will have to manually migrate old ones to the new version otherwise they will continue attempting to run with the older flow. If it's the same definition nothing will be done. We'll just rely on the old defined instance. Workflow definitions are intended to be immutable once they are created. The server will send the `definition-id` of this resolved version to the client, since all subsequent interactions with this workflow have to be specified in terms of the definition id.

On a separate machine, the carabiner-rs is running in `worker` mode. Unlike our client singleton we will launch on the first available grpc port - we don't need to ensure the 1-instance-per-host trait like we do for the implicitly launched python clients. At launch time this will spawn N different Python interpreters set to a CLI entrypoint of our carabiner-py package. This entrypoint will connect to the grpc server and loop indefinitely. The server adds each of these connections to a pool with a long running bidirectional grpc link. It's intended to receive work, handle work, and report results. It has no database communication - all work is passed by our server. In other job queues this would be known as the job broker.

carabiner-rs polls the database centrally so we only need 1 connection per host machine. It finds actions that haven't yet been completed and distributes them to the connected grpc worker clients. The clients read these jobs, import the necessary modules with `importlib` since we should be running in a virtualenv that has the user packages installed, runs the logic, and sends the results back to grpc.

The server receives these completed actions and uses them to increment a state machine build off of the workflow DAG. This state machine determines if any other actions have been "unlocked" by the completion of this action, or if we need to wait for subsequent actions. If they have been unlocked we will insert them into the database for subsequent queuing. The cycle continues until we have a final result that can be set as the output of the full workflow instance. At that point we can wake up any waiting callers.

## Workers

The `start_workers` binary polls for the work to be done. Launch this a single
time for each physical worker node you have in your cluster. Worker processes read their
configuration from `DATABASE_URL` plus optional `CARABINER_*` environment
variables (poll interval, batch size, worker count, etc.) so the loop can be
tuned per deployment without CLI flags. The dispatcher shares a single
`Database` handle with the worker bridge, repeatedly calls `dispatch_actions()`
to dequeue pending actions, and streams the resulting payloads through the
round-robin worker pool. Workers still have no direct database access – they
only handle gRPC traffic from the dispatcher.

```
         +-------------------+       SQL poll/update      +------------+
         | Polling Dispatcher| -------------------------> | PostgreSQL |
         |   (dispatch loop) | <------------------------- |   Ledger   |
         +-------------------+                           +------------+
                    |
                    | gRPC dispatch/results
                    v
         +-------------------+      gRPC only      +-----------------+
         | PythonWorkerPool  | <-----------------> | Python workers  |
         |   (bridge server) |                    | (carabiner-worker)
         +-------------------+                    +-----------------+
```

Tuning notes:

- `poll_interval_ms` balances latency and database load. The 100ms default
  yields at most 10 queries per second per worker process while keeping queued
  actions responsive.
- `batch_size` controls how many dequeued actions get fanned out per poll. 100
  gives ~1000 actions/second at the default interval without overwhelming the
  worker pool.
- `max_concurrent` dispatches default to `workers * 2`, providing enough
  in-flight buffering to keep workers busy without unbounded task spawning.

The dispatcher automatically handles completion batching and graceful
shutdowns so a single host can service the entire queue with one outbound
connection.
