# Optimization Scratch

Date: 2026-01-14

## Goals
- Increase action throughput for queue-noop benchmark.
- Reduce database hot-path work in start/dispatch/completion.

## Baseline (recent)
- queue-noop (1k instances, 10s): ~701 actions/sec after completion update+flush.
- queue-noop (500k instances, 60s): ~56 actions/sec (timeout/no clean shutdown).

## Notes
- start_unstarted_instances currently scans `workflow_instances` with `NOT EXISTS` on `action_queue`.
- completion path now marks `action_queue` status = completed and defers log flush.

## Experiments
- TBD

## Change 1: Claim unstarted instances
- Added workflow_instances.started_at to claim start work and avoid NOT EXISTS scan.
- find_unstarted_instances now updates started_at with SKIP LOCKED claim.
- Added clear_instance_started_at on start errors.

### Benchmark results (post started_at claim)
- queue-noop, 1k instances, 10s: 6908 actions / 10.07s (~685.7 actions/sec)
- queue-noop, 500k instances, 60s: 5840 actions / 60.05s (~97.3 actions/sec)
- 500k run still spams "Failed to send completion: channel closed" and does not exit before timeout.

### Metrics snapshot (queue-noop, 1k instances, 10s)
- process_completion_db_avg_us: ~21094us
- process_completion_inbox_avg_us: ~4611us
- start_unstarted_avg_us: ~41859us per call

## Change 2: Inbox cache (best-effort)
- Added per-instance in-memory cache for non-spread inbox values.
- process_completion_unified uses cache and only hits DB for missing nodes.
- Cache updated after successful execute_completion_plan and cleared on workflow completion.

### Metrics snapshot (queue-noop, 1k instances, 10s, cache)
- process_completion_inbox_avg_us: ~4113us (down from ~4611us)
- process_completion_db_avg_us: ~19232us (down from ~21094us)
- throughput observed: ~753 actions/sec

### Benchmark results (post inbox cache)
- queue-noop, 1k instances, 10s: 7570 actions / 10.05s (~753.3 actions/sec)
- queue-noop, 500k instances, 60s: 5960 actions / 60.06s (~99.2 actions/sec)
- 500k run still spams "Failed to send completion: channel closed" and times out.

## Change 3: Completion send log suppression
- Downgraded completion send failure log to debug to reduce shutdown spam.
- Not re-ran 500k benchmark after this change yet.

## Trace analysis (RUST_LOG=rappel::runner=info, 1k instances, timeout 5s)
- Trace file: docs/benchmark_trace_info.json
- Span CPU totals (from B/E pairs):
  - process_completion_unified: ~2.83s total CPU (avg ~33us per poll)
  - start_unstarted_instances: ~1.37s total CPU (avg ~25us per poll)
  - fetch_and_dispatch: ~0.18s total CPU
- Spans are async, so most elapsed time is waiting on DB I/O rather than CPU.
- Runner metrics (from cache run) still show DB time per completion ~19ms, which aligns with I/O-bound behavior.

## Change 4: Batched completion flush
- Added CompletionBatcher to aggregate completion plans and write via execute_completion_plans_batch.
- Completion plan writes now bulk update action_queue, node_inputs, node_readiness, and action_queue inserts.

### Benchmark results (post completion batching)
- queue-noop, 1k instances, 10s, 4 workers: 7506 actions / 10.02s (~749.5 actions/sec)
- queue-noop, 1k instances, 10s, 4 workers + metrics: 9274 actions / 10.04s (~924.2 actions/sec)
- queue-noop, 1k instances, 10s, 16 workers: 10924 actions / 10.02s (~1090.1 actions/sec)
- queue-noop, 500k instances, 60s, 16 workers: 6848 actions / 60.05s (~114.0 actions/sec), runner hang (worker pool references) and command timed out at 180s.

### Metrics snapshot (queue-noop, 1k instances, 10s, batch)
- process_completion_db_avg_us: ~39456us (includes batch wait time)
- process_completion_inbox_avg_us: ~4022us

## Change 5: Completion batching across instances (no per-instance limit)
- Removed per-instance limit in CompletionBatcher flush; allows multiple completions per instance in a single batch.
- Always route completion plans through CompletionBatcher (batching already handles readiness resets).

### Benchmark results (post batcher relax)
- queue-noop, 1k instances, 10s, 4 workers: 16407 actions / 10.04s (~1634.8 actions/sec)

## Change 6: Batch instance start plans
- Build initial completion plans for all claimed instances, then execute via execute_completion_plans_batch.
- Reduce per-instance start overhead and avoid per-instance transactions.
- Log start events at debug to avoid per-instance info spam.

### Benchmark results (post batched start)
- queue-noop, 10k instances, 10s, 4 workers: 6200 actions / 10.06s (~616.5 actions/sec)
- queue-noop, 50k instances, 10s, 4 workers: 5240 actions / 10.02s (~523.0 actions/sec)

## Change 7: Avoid per-instance DAG fetch during start
- Use workflow_version_id from claimed instances to fetch DAG directly and cache instance->version.

### Benchmark results (post DAG cache warm)
- queue-noop, 10k instances, 10s, 4 workers: 13211 actions / 10.05s (~1314.0 actions/sec)
- queue-noop, 50k instances, 10s, 4 workers: 7120 actions / 10.06s (~707.9 actions/sec)
