# DAG Runner

The runner is the runtime that polls Postgres, dispatches work to Python workers, executes inline nodes, and keeps the DAG moving.

## Big pieces

- **DAG cache**: caches DAGs by workflow version so we do not decode the proto on every completion.
- **Execution graph**: single protobuf blob per instance containing all runtime state.
- **Worker slot tracker**: per-worker capacity accounting for concurrent actions.
- **Python worker pool**: long-lived gRPC connections to Python workers.
- **In-flight tracker**: maps delivery tokens to dispatched actions and watches timeouts.
- **Completion handler**: applies completions to the execution graph and determines newly ready nodes.

## Data in Postgres

- `workflow_instances`: active instances with stripped execution graphs (work queue).
- `workflow_versions`: immutable DAG definitions.
- `node_payloads`: action inputs/results for cold start recovery (INSERT-only).
- `completed_instances`: archived workflows with full execution graphs.
- `workflow_schedules`: cron/interval schedule definitions.
- `worker_status`: per-pool throughput and health metrics.

## Execution graph

Each instance has a single execution graph that tracks:

- **Node map**: status, attempts, inputs, results for each node
- **Ready queue**: nodes waiting to be dispatched
- **Variables**: workflow scope (inputs, intermediate values, outputs)
- **Exceptions**: caught and uncaught errors

The graph is stored as a compressed protobuf blob. During active execution, we store **stripped graphs** (~2KB) without payload data. Full graphs with all inputs/outputs are only written to `completed_instances` when workflows finish.

## Claim + dispatch loop

The runner operates in a tight loop:

1. **Claim instances**: acquire ownership of unclaimed instances via lease
2. **Process completions**: apply worker results to execution graphs
3. **Collect ready nodes**: find nodes ready to dispatch
4. **Dispatch to workers**: send actions to Python workers
5. **Finalize**: persist state changes, complete/fail finished instances

Each claimed instance is held in memory with its execution graph. The runner maintains a lease that must be periodically renewed. If a runner crashes, its leases expire and another runner can reclaim the instances.

## Completion flow

When an action completes:

1. Mark the node as completed in the execution graph
2. Store result in the node and update variables
3. Evaluate outgoing edges and guards
4. Mark successor nodes as ready if all predecessors are done
5. For barriers/joins, aggregate predecessor results

The execution graph handles all state transitions in memory. Changes are batched and persisted periodically rather than on every completion.

## Storage tiers

**Hot path** (workflow_instances)
- Stripped graphs: node statuses, ready queue, variables
- No payload data (inputs/results stripped)
- Fast reads/writes during active execution

**Warm storage** (node_payloads)
- Action inputs and results
- INSERT-only, no updates
- Used for cold start recovery when reclaiming instances
- Cleaned up when instances complete

**Cold storage** (completed_instances)
- Full graphs with all inputs/outputs
- Written asynchronously on completion
- Supports dashboard and historical queries

## Spread, parallel, and joins

- Spread nodes create N action instances (one per item)
- Each spread action writes its result with a spread index
- Barrier nodes wait for all predecessors, then aggregate results
- Join nodes with `required_count = 1` are inline; multi-predecessor joins are barriers

## Failures, retries, and timeouts

- Action failures route through exception handling to either retry or propagate
- Retry + timeout policies come from the DAG node (see `docs/Action-Retries.md`)
- A background timeout loop marks timed-out actions and triggers retries or failures
- Exhausted retries without an exception handler fail the workflow
