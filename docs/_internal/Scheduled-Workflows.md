# Scheduled Workflows

Waymark supports recurring workflow execution via cron or interval schedules.

## What a schedule targets

Schedules are keyed by `(workflow_name, schedule_name)`.

- `workflow_name` comes from `Workflow.short_name()`.
- `schedule_name` is required and lets one workflow have multiple schedules.

At fire time, the scheduler resolves the DAG by workflow name and uses the most recently created workflow version in `workflow_versions`.

## Python API

### Create or update a schedule

```python
from datetime import timedelta
from waymark import Workflow, workflow, schedule_workflow


@workflow
class DataSyncWorkflow(Workflow):
    name = "data_sync"

    async def run(self, region: str) -> None:
        ...


# Cron schedule
schedule_id = await schedule_workflow(
    DataSyncWorkflow,
    schedule_name="hourly-us-east",
    schedule="0 * * * *",
    inputs={"region": "us-east"},
)

# Interval schedule
schedule_id = await schedule_workflow(
    DataSyncWorkflow,
    schedule_name="every-5-min",
    schedule=timedelta(minutes=5),
    inputs={"region": "us-west"},
)
```

### Pause/resume/delete a schedule

```python
from waymark import pause_schedule, resume_schedule, delete_schedule

await pause_schedule(DataSyncWorkflow, schedule_name="hourly-us-east")
await resume_schedule(DataSyncWorkflow, schedule_name="hourly-us-east")
await delete_schedule(DataSyncWorkflow, schedule_name="hourly-us-east")
```

### List schedules

```python
from waymark import list_schedules

all_schedules = await list_schedules()
active_only = await list_schedules(status_filter="active")
paused_only = await list_schedules(status_filter="paused")
```

## Schedule semantics

### Cron

Waymark accepts standard 5-field cron syntax and normalizes to 6 fields internally.

Examples:

- `0 * * * *`: hourly
- `*/15 * * * *`: every 15 minutes
- `0 0 * * *`: daily at midnight

### Interval

For interval schedules, next run is:

- `now + interval` on creation
- `last_run_at + interval` after each successful fire

Optional jitter adds a random delay in `[0, jitter_seconds]`.

## Persistence model

Schedules are stored in `workflow_schedules` with:

- scheduling fields (`schedule_type`, `cron_expression`, `interval_seconds`, `jitter_seconds`)
- state fields (`status`, `next_run_at`, `last_run_at`, `last_instance_id`)
- behavior fields (`priority`, `allow_duplicate`)

Upsert behavior (`workflow_name`, `schedule_name` conflict):

- updates schedule settings
- recomputes `next_run_at`
- sets `status = 'active'`

## Runtime behavior

- Due schedules are polled by the scheduler task and converted to normal queued workflow instances.
- Input payload is converted into input assignments in the new instance `RunnerState`.
- After queueing, schedule execution metadata is updated (`last_run_at`, `last_instance_id`, next `next_run_at`).
- For `allow_duplicate=false`, Postgres suppresses overlap by checking for existing queued instances tied to the same `schedule_id` before firing a new run.
