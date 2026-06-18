# Action Retries

Waymark supports action retry policies, but enforcement today is split:

- Parsed policy surface: retry count, exception filters, backoff, timeout
- Runtime-enforced policy: retry count + exception filters

This document describes what is actually implemented now.

## Policy surface (IR + Python API)

In IR, policy brackets are attached to action calls:

```waymark
# Catch all exceptions, retry up to 3 times with 60s exponential backoff
result = @risky_action() [retry: 3, backoff: 60]

# Catch specific exception type
result = @network_call() [NetworkError -> retry: 5, backoff: 2m]

# Multiple exception types
result = @api_call() [(ValueError, KeyError) -> retry: 3, backoff: 30s]

# Multiple policies for different exceptions
result = @api_call() [RateLimitError -> retry: 10, backoff: 1m] [NetworkError -> retry: 3, backoff: 30s]

# Timeout in separate bracket
result = @slow_action() [retry: 3, backoff: 60] [timeout: 2m]
```

Python API maps onto those same IR policies via `Workflow.run_action(...)`:

```python
await self.run_action(
    my_action(),
    retry=RetryPolicy(
        attempts=3,
        exception_types=["NetworkError"],
        backoff_seconds=60,
    ),
    timeout=timedelta(minutes=2),
)
```

Python retry defaults in the IR builder:

- `RetryPolicy(attempts=N)` maps to `max_retries = N - 1` (`attempts` is total executions).
- `RetryPolicy()` (no `attempts`) maps to `max_retries = 100`.

## Runtime behavior implemented today

Action failure handling happens in `RunnerExecutor`:

1. Action result is treated as failure if payload looks like an exception object (`type` / `message`).
2. Retry policy match is computed from:
   - current `action_attempt`
   - matched `retry` brackets on the action node
   - optional exception type filter
3. If retry is allowed:
   - increment `action_attempt`
   - set node status back to `queued`
   - re-enqueue the same execution node
4. If retry is exhausted:
   - mark node `failed`
   - propagate via exception edges

Retry decision logic today:

- `max_retries` is the max across matching retry brackets.
- Attempt counter starts at `1`.
- Retry allowed when `attempt - 1 < max_retries`.

## What gets persisted

Retry metadata is persisted in graph state, not in a separate action queue table.

- `runner_instances.state` stores each execution node (including `action_attempt`).
- `runner_actions_done` stores successful action outputs as append-only rows:
  - `execution_id`
  - `attempt`
  - `result`
- Failed attempts are not inserted into `runner_actions_done`.

When reclaiming an instance, the backend restores latest successful action output per `execution_id` from `runner_actions_done`.

## Current limitations

These fields are parsed but currently not enforced by the runtime dispatch path:

- Retry backoff delay (`backoff`) is not applied before requeue.
- Timeout policy (`timeout`) is not enforced by the runloop scheduler.
- Worker dispatch currently sends `timeout_seconds = 0` and `max_retries = 0` in `ActionDispatch`.

In short: retries are currently immediate and executor-driven.
