# Architecture

Waymark is built around a Postgres-backed instance queue plus an in-memory runner.

High-level flow:

`Python SDK -> waymark-bridge (gRPC) -> Postgres -> start-workers runloop -> Python worker pool`

## Runtime components

- `waymark-bridge`: accepts workflow registrations and queue requests from the Python SDK.
- `start-workers`: owns the runloop, scheduler, worker bridge, status reporter, and webapp server.
- `RemoteWorkerPool`: dispatches action calls to Python worker processes over gRPC.
- `PostgresBackend`: shared persistence implementation for core runtime, schedules, and webapp queries.

## Current persistence model

There are six primary tables in active use:

- `workflow_versions`: immutable workflow IR payloads (`program_proto`) keyed by `(workflow_name, workflow_version)`.
- `queued_instances`: dequeue/claim table with `scheduled_at`, `lock_uuid`, and `lock_expires_at`.
- `runner_instances`: per-instance state snapshot (`state`) plus terminal `result`/`error`.
- `runner_actions_done`: append-only completed action results by `(execution_id, attempt)`.
- `workflow_schedules`: recurring schedule definitions and run metadata.
- `worker_status`: per-pool throughput and latency snapshots.

State payloads are serialized as MessagePack (`rmp_serde`) byte blobs, not protobuf blobs.

## Insert and upsert strategy

### Workflow registration

`WorkflowRegistryBackend::upsert_workflow_version` does:

1. `INSERT ... ON CONFLICT (workflow_name, workflow_version) DO NOTHING RETURNING id`
2. If conflict, `SELECT id, ir_hash`
3. Reject if `ir_hash` changed for the same `(workflow_name, workflow_version)`

This keeps version keys immutable while preserving idempotent registration.

### Queueing new instances

`CoreBackend::queue_instances` writes both queue and runner rows in one transaction:

1. Batch insert into `queued_instances(instance_id, scheduled_at, payload)`
2. Batch insert into `runner_instances(instance_id, entry_node, workflow_version_id, state)`

This dual-write in a single transaction prevents queue rows from existing without a corresponding runner state row.

### Claiming work

`CoreBackend::get_queued_instances` uses `FOR UPDATE SKIP LOCKED` to claim due rows:

- selects rows where `scheduled_at <= now` and lock is missing/expired
- sets `lock_uuid` + `lock_expires_at`
- joins `runner_instances` to hydrate graph state

On hydrate, it backfills `action_results` by selecting the latest row per `execution_id` from `runner_actions_done` (`DISTINCT ON ... ORDER BY attempt DESC, id DESC`).

### Persisting progress

Runloop persistence order is intentional:

1. Insert `runner_actions_done` rows first (append-only history)
2. Update `runner_instances.state` for claimed rows
3. Update `queued_instances.scheduled_at` from `GraphUpdate::next_scheduled_at()`

The state updates are lock-gated (`WHERE qi.lock_uuid = $lock_uuid` and unexpired), so stale owners cannot overwrite live state.

### Completing instances

`CoreBackend::save_instances_done`:

1. Batch updates `runner_instances.result/error`
2. Deletes matching rows from `queued_instances`

This keeps completed instances queryable in `runner_instances` while removing them from the active queue.

### Schedules and worker status

- `upsert_schedule`: `INSERT ... ON CONFLICT (workflow_name, schedule_name) DO UPDATE`, recalculates `next_run_at`, and resets status to `active`.
- `upsert_worker_status`: `INSERT ... ON CONFLICT (pool_id, worker_id) DO UPDATE` for rolling metrics.

## Instance lifecycle

1. Python workflow class compiles to IR and calls bridge registration.
2. Bridge upserts `workflow_versions` and queues a `QueuedInstance`.
3. Runloop claims due instances, hydrates DAG + state, and fans work into executor shards.
4. Action completions update in-memory state; durable deltas are persisted through `PostgresBackend`.
5. Finished instances are written to `runner_instances.result/error` and removed from `queued_instances`.

## Locking and recovery

- Locks are leases on `queued_instances` rows (`lock_uuid`, `lock_expires_at`).
- A heartbeat refreshes lock expiry for active in-memory instances.
- An expired-lock reclaimer periodically clears expired leases in batches (`FOR UPDATE SKIP LOCKED`).
- If lock ownership changes, the runloop evicts that in-memory executor and releases local state.

## Scheduler behavior

Schedules are keyed by `(workflow_name, schedule_name)` and resolve workflows by name.

- The scheduler fetches due schedules from `workflow_schedules`.
- It resolves a DAG by loading the most recent `workflow_versions` row for that `workflow_name` (`ORDER BY created_at DESC LIMIT 1`).
- It queues a normal `QueuedInstance` and then marks schedule execution (`last_run_at`, `last_instance_id`, next `next_run_at`).
- For schedules with `allow_duplicate=false`, Postgres checks for an existing queued instance for the same `schedule_id` before firing a new run.
