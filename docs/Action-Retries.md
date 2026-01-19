# Action Retries

Actions support retry policies for automatic error recovery. Retry configuration exists at two levels: the Rappel IR syntax and the Python workflow API via `run_action`.

## IR Syntax

In the Rappel IR, retry policies are specified in brackets after action calls. Timeout is a separate bracket since it's independent of exception handling:

```rappel
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

Retry parameters:
- `retry: N` - Maximum retry attempts before permanent failure
- `backoff: DURATION` - Base backoff duration (exponential: `backoff * 2^attempt`)

Duration formats: bare numbers are seconds, or use suffixes like `30s`, `2m`, `1h`.

## Python Workflow API

Retry and timeout policies are attached at the call site using `Workflow.run_action`. The action itself is still decorated with `@action`, but retries are not configured on the decorator.

```python
from datetime import timedelta

from rappel import action, workflow
from rappel.workflow import RetryPolicy, Workflow


@action
async def my_action() -> str:
    ...


@workflow
class ExampleWorkflow(Workflow):
    async def run(self) -> str:
        return await self.run_action(
            my_action(),
            retry=RetryPolicy(
                attempts=3,
                exception_types=["NetworkError"],
                backoff_seconds=60,
            ),
            timeout=timedelta(minutes=2),
        )
```

Parameters:
- `RetryPolicy.attempts`: Total attempts (initial try + retries)
- `RetryPolicy.exception_types`: Exception type names to match; empty/None catches all
- `RetryPolicy.backoff_seconds`: Base backoff duration in seconds
- `timeout`: Per-attempt timeout (int/float seconds or `timedelta`)

## Database Representation

The `action_queue` table stores retry configuration per action:

| Column | Type | Description |
|--------|------|-------------|
| `max_retries` | int | Maximum retry attempts |
| `attempt_number` | int | Current attempt (0-indexed) |
| `timeout_seconds` | int | Per-attempt timeout |
| `timeout_retry_limit` | int | Retries remaining for timeout failures |
| `backoff_kind` | enum | `none`, `linear`, or `exponential` |
| `backoff_base_delay_ms` | int | Base delay for backoff calculation |
| `retry_kind` | string | What triggered the retry |

## Runtime Behavior

Timeouts are stored as `NOW() + timeout` in the database, allowing the runner to fetch timed-out actions:

```sql
SELECT ... WHERE timeout_at < NOW() AND status = 'dispatched' FOR UPDATE SKIP LOCKED
```

Each runnable action gets a `delivery_token` (UUID) assigned at dispatch time. Only the owner holding this token can update the action status, preventing race conditions when multiple runners poll the queue.

When an action times out or fails:

1. The handler checks if retries remain
2. If retries remain:
   - Decrement the retry count
   - Calculate next attempt time: `base_delay * 2^attempt` for exponential backoff
   - Re-queue with `scheduled_at` set to the backoff time
3. If no retries remain:
   - Mark action as permanently failed
   - Trigger exception handling in the DAG (propagate to handlers or fail the workflow)

## Exception-Specific Policies

The IR supports different retry policies per exception type. At runtime:

1. When an action fails, the exception type is extracted from the result
2. The runner checks outgoing edges from the action node for matching `exception_types`
3. If a matching policy exists with retries remaining, the action is re-queued
4. If no match or retries exhausted, the exception propagates to handlers defined in the DAG
