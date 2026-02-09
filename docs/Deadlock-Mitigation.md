# Deadlock Mitigation and Queue/Runner Refactor Plan

## Context

Soak failures showed repeated Postgres deadlocks and eventual runloop stall:

- `error returned from database: deadlock detected` repeated in `start-workers.log`
- `actions_per_sec` dropped to `0.0` while `ready_queue` kept increasing
- runloop logged `runloop stopping due to error` but did not restart

The goals below are:

- prevent one transient database deadlock from freezing the worker supervisor
- reduce deadlock probability under high concurrency
- simplify table ownership boundaries so lock behavior is easier to reason about

## Phase 1: No-Brainer Hardening

### 1) Make instance poller send stop-aware (DONE)

Current risk:

- `instance_tx.send(message).await` can block (`src/waymark_core/runloop.rs:808`)
- runloop shutdown awaits the poller task (`src/waymark_core/runloop.rs:1441`)
- if poller is blocked while runloop has stopped consuming, supervisor restart never executes (`src/waymark_core/runloop.rs:1527`)

Change:

- replace bare send with a `tokio::select!` that also listens to stop notification (same pattern used by completion task)
- treat send as cancellable on shutdown

Expected effect:

- runloop always returns to supervisor
- deadlock errors can recover via restart instead of wedging

### 2) Add bounded retry for transient Postgres deadlocks (DONE)

Current risk:

- single deadlock error immediately tears down the runloop

Change:

- add small bounded retry with jitter/backoff for SQLSTATE `40P01` (deadlock detected)
- optionally include SQLSTATE `40001` (serialization failure)
- apply around hot backend operations:
  - `get_queued_instances_impl` (`src/backends/postgres/core.rs:544`)
  - `save_graphs_impl` (`src/backends/postgres/core.rs:399`)
  - `save_instances_done_impl` (`src/backends/postgres/core.rs:692`)
  - `refresh_instance_locks` (`src/backends/postgres/core.rs:774`)

Expected effect:

- deadlocks become transient throughput dips rather than control-plane failures

### 3) Normalize lock/write order across queue and runner tables (DONE)

Current risk:

- lock order inversion increases deadlock cycles:
  - state persist path touches `runner_instances` then `queued_instances` (`src/backends/postgres/core.rs:423`, `src/backends/postgres/core.rs:450`)
  - dequeue path touches `queued_instances` then `runner_instances` (`src/backends/postgres/core.rs:569`, `src/backends/postgres/core.rs:598`)

Change:

- adopt one global order for code paths that must touch both tables
- document and enforce it in backend code review checklist

Expected effect:

- materially lower deadlock frequency in high-concurrency paths

## Phase 2: Maintenance Write Hygiene

### 4) Make non-durable maintenance updates best-effort and skip-locked (DONE)

Principle:

- if an update is not required for immediate durability of workflow state, avoid waiting on locks

Apply this to:

- lock heartbeat refresh path (`src/backends/postgres/core.rs:784`)
- lock reclaim/requeue paths (keep `FOR UPDATE SKIP LOCKED` behavior)

Implementation direction:

- heartbeat refresh can claim rows via `FOR UPDATE SKIP LOCKED` and update only acquired rows
- missing one heartbeat tick is acceptable because next tick can refresh, and durable writes can extend leases

### 5) Coalesce lease refresh with durable state writes (DONE)

Idea:

- whenever runloop already writes owned rows for durable progress, also extend `lock_expires_at` in that write path

Why:

- reduces separate heartbeat write pressure
- shrinks lock-conflict surface area
- aligns with principle that durable writes get priority, maintenance writes are best-effort

## Phase 3: Structural Simplification

### 6) Clarify `queued_instances` vs `runner_instances` ownership (DONE)

Current practical split:

- `queued_instances`: scheduling + leasing surface (`scheduled_at`, `lock_uuid`, `lock_expires_at`)
- `runner_instances`: durable execution snapshot and terminal outcome (`state`, `result`, `error`)

Current issue:

- both tables carry `current_status`, creating cross-table write coupling on hot paths

Recommendation:

- make one table canonical for status (prefer `runner_instances`)
- keep `queued_instances` focused on queue/lease mechanics
- for metadata searching, use:
  - read-time joins/views, or
  - derived projection tables updated asynchronously

### 7) Reduce cross-table writes in hot paths (DONE)

Direction:

- queue path writes only queue/lease fields where possible
- runner path writes durable execution state
- avoid synchronized status writes to both tables in same transaction unless strictly required

Expected effect:

- fewer lock overlaps
- smaller deadlock graph
- easier correctness reasoning
