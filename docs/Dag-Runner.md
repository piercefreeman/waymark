# DAG Runner

The runner is the runtime that polls Postgres, dispatches work to Python workers, executes inline nodes, and keeps the DAG moving.

## Big pieces

- **DAG cache**: caches DAGs by workflow version so we do not decode the proto on every completion.
- **Work queue handler**: polls `action_queue`, respects worker capacity, and dispatches work.
- **Worker slot tracker**: per-worker capacity accounting for concurrent actions.
- **Python worker pool**: long-lived gRPC connections to Python workers.
- **In-flight tracker**: maps delivery tokens to dispatched actions and watches timeouts.
- **Completion handler + batcher**: builds completion plans and commits them in a single transaction.

## Data in Postgres

- `action_queue`: runnable nodes (`action`, `barrier`, `sleep`).
- `node_inputs`: inbox storage (append-only; latest value wins).
- `node_readiness`: per-node counters for the unified readiness model.
- `workflow_instances` / `workflow_versions`: instance state and DAG bytes.
- `action_logs`: audit trail for completed actions.

## Polling + dispatch

- The runner calls `dispatch_runnable_nodes` with the available slot count.
- `action` nodes are dispatched to Python workers.
- `barrier` nodes (aggregators and multi-predecessor joins) execute inline.
- `sleep` nodes are handled inline once their `scheduled_at` time arrives.
- Each dispatched action gets a delivery token for idempotent completion.

## Inline scope + inbox

- Workflow inputs are seeded into an inline scope and also written to downstream inboxes so actions can access inputs even if no intermediate node re-emits them.
- The inbox (`node_inputs`) is append-only; reads pick the latest value per variable (plus `spread_index` for aggregators).
- The runner caches initial scopes and inbox snapshots per instance to cut down on round trips.

## Completion flow (unified readiness model)

When an action or barrier completes, the runner:

1. Analyzes the reachable subgraph (inline nodes vs frontier nodes).
2. Batch-reads inbox data for those nodes.
3. Executes inline nodes in memory, updating the scope and collecting inbox writes.
4. Commits a completion plan in one transaction: inbox writes, readiness increments, enqueue newly ready nodes, and optional instance completion.

Frontier nodes are categorized as:

- **Action**: external work sent to Python.
- **Barrier**: aggregators and joins that must wait for multiple predecessors.
- **Output**: final workflow completion.

## Spread, parallel, and joins

- Spread/parallel aggregators are enqueued as `barrier` nodes once all predecessors complete.
- Join nodes with `required_count = 1` are treated as inline; joins with multiple predecessors become barriers.

## Failures, retries, and timeouts

- Action failures route through `CompletionEngine.handle_action_failure` to either requeue or route to exception handlers.
- Retry + timeout policies come from the DAG node (see `docs/Action-Retries.md`).
- A background timeout loop marks timed-out actions and requeues or fails instances as needed.
