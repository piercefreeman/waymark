# DAG Runner

The runner coordinates durable workflow execution from queued instance claim through completion.

## Core pieces

- `RunLoop` supervisor with restart/backoff.
- Executor shards (OS threads) for CPU-bound graph advancement.
- Shared `CoreBackend` for queue claims and durable writes.
- `WorkflowRegistryBackend` for DAG hydration from `workflow_versions`.
- `RemoteWorkerPool` for Python action dispatch/completion.
- Lock heartbeat + lock reclaimer for lease ownership.

## Runtime state model

Each active instance owns one runtime execution graph:

- `nodes`: execution nodes keyed by `execution_id` (UUID)
- `edges`: runtime edges
- action attempt counters (`action_attempt`) on action nodes
- queued/running/completed/failed status
- optional `scheduled_at` for sleep/delay semantics

Durable snapshot type: `GraphUpdate { instance_id, nodes, edges }`.
Stored in `runner_instances.state` as MessagePack.

## Postgres tables used by the runner

- `queued_instances`: active queue rows, scheduling, and lock lease columns
- `runner_instances`: current graph snapshot plus final result/error
- `runner_actions_done`: append-only successful action outputs by attempt
- `workflow_versions`: IR payload for DAG hydration

## Claim and hydrate

Claiming is done with `FOR UPDATE SKIP LOCKED`:

1. Pick due queue rows (`scheduled_at <= now`) with no lock or expired lock.
2. Set lock ownership (`lock_uuid`, `lock_expires_at`).
3. Join to `runner_instances.state`.
4. Decode queued payload + graph snapshot.

Hydration also restores prior successful action outputs by querying the latest `runner_actions_done` record per `execution_id`.

## Step execution

Per event batch (new claims, action completions, sleep wakes), shards:

1. apply finished node updates
2. execute inline nodes in-memory
3. emit newly runnable action requests
4. emit durable deltas:
   - `ActionDone` rows
   - `GraphUpdate` snapshots
5. emit `InstanceDone` when terminal

## Durable write ordering

Write ordering is explicit:

1. insert `runner_actions_done` first
2. update `runner_instances.state`
3. update `queued_instances.scheduled_at`

If lock ownership no longer matches, the instance is evicted from memory and its lock is released.

## Completion path

When an instance finishes:

1. write terminal `result` or `error` to `runner_instances`
2. delete its row from `queued_instances`

The completed instance remains queryable in `runner_instances`.

## Lock lifecycle

- Claim: set `lock_uuid` + `lock_expires_at`
- Heartbeat: extend `lock_expires_at` for owned instances
- Release: clear lock columns when evicting/shutting down
- Reclaim: periodic sweep clears expired locks in batches

## Sleep and eviction behavior

Sleep nodes set per-node `scheduled_at` in graph state.
If an instance is blocked on long sleeps (beyond `WAYMARK_EVICT_SLEEP_THRESHOLD_MS`) and has no inflight actions, runloop evicts it and releases the queue lock. It can be reclaimed later from durable state.

## Retries and failures

- Retry decisions are evaluated in `RunnerExecutor` from action policy brackets.
- Retry increments `action_attempt` and re-queues the same execution node.
- Exhausted retries mark the action as failed and propagate exception edges.
- Successful attempts are appended to `runner_actions_done`.

Details and current limitations are documented in `docs/Action-Retries.md`.
