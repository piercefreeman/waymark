# Scheduled Workflows

Rappel supports scheduling workflows to run automatically on a recurring basis. You define your workflow once, then register a schedule to have it execute at regular intervals or according to a cron expression.

## Overview

Scheduled workflows are useful for:

- **Periodic data processing**: Run ETL jobs every hour
- **Maintenance tasks**: Clean up stale data nightly
- **Report generation**: Generate daily/weekly reports
- **Health checks**: Monitor external services at fixed intervals

Schedules are tied to workflow names, not specific versions. When a schedule fires, Rappel executes the latest registered version of that workflow.

## Python API

### Scheduling a Workflow

Use `schedule_workflow` to register a recurring schedule:

```python
from datetime import timedelta
from rappel import Workflow, action, workflow, schedule_workflow

@action
async def fetch_data() -> dict:
    # Fetch data from external source
    return {"items": [...]}

@action
async def process_batch(data: dict) -> int:
    # Process the fetched data
    return len(data["items"])

@workflow
class DataSyncWorkflow(Workflow):
    name = "data_sync"

    async def run(self) -> int:
        data = await fetch_data()
        return await process_batch(data)


# Schedule with a cron expression (runs at minute 0 of every hour)
schedule_id = await schedule_workflow(
    DataSyncWorkflow,
    schedule="0 * * * *"
)

# Or schedule with an interval (runs every 5 minutes)
schedule_id = await schedule_workflow(
    DataSyncWorkflow,
    schedule=timedelta(minutes=5)
)
```

### Passing Inputs to Scheduled Runs

You can provide inputs that will be passed to each scheduled execution:

```python
@workflow
class ReportWorkflow(Workflow):
    name = "daily_report"

    async def run(self, region: str, include_debug: bool = False) -> str:
        # Generate report for the specified region
        ...

# Each scheduled run receives these inputs
await schedule_workflow(
    ReportWorkflow,
    schedule="0 0 * * *",  # Daily at midnight
    inputs={"region": "us-east", "include_debug": True}
)
```

### Managing Schedules

#### Pause and Resume

Temporarily stop a schedule without deleting it:

```python
from rappel import pause_schedule, resume_schedule

# Pause the schedule (it won't fire until resumed)
await pause_schedule(DataSyncWorkflow)

# Resume the schedule
await resume_schedule(DataSyncWorkflow)
```

#### Delete a Schedule

Remove a schedule entirely:

```python
from rappel import delete_schedule

await delete_schedule(DataSyncWorkflow)
```

Deleted schedules can be recreated by calling `schedule_workflow` again.

#### List Schedules

Query all registered schedules:

```python
from rappel import list_schedules, ScheduleInfo

# List all non-deleted schedules
schedules = await list_schedules()
for s in schedules:
    print(f"{s.workflow_name}: {s.status} (next run: {s.next_run_at})")

# Filter by status
active_schedules = await list_schedules(status_filter="active")
paused_schedules = await list_schedules(status_filter="paused")
```

## Schedule Types

### Cron Expressions

Standard 5-field cron expressions:

```
┌───────────── minute (0-59)
│ ┌───────────── hour (0-23)
│ │ ┌───────────── day of month (1-31)
│ │ │ ┌───────────── month (1-12)
│ │ │ │ ┌───────────── day of week (0-6, Sunday=0)
│ │ │ │ │
* * * * *
```

Examples:

| Expression | Description |
|------------|-------------|
| `0 * * * *` | Every hour at minute 0 |
| `*/15 * * * *` | Every 15 minutes |
| `0 0 * * *` | Daily at midnight |
| `0 9 * * 1-5` | Weekdays at 9 AM |
| `0 0 1 * *` | First day of each month at midnight |

### Interval Schedules

Use `timedelta` for fixed-interval scheduling:

```python
from datetime import timedelta

# Every 30 seconds
await schedule_workflow(MyWorkflow, schedule=timedelta(seconds=30))

# Every 2 hours
await schedule_workflow(MyWorkflow, schedule=timedelta(hours=2))

# Every 1 day and 6 hours
await schedule_workflow(MyWorkflow, schedule=timedelta(days=1, hours=6))
```

Interval schedules compute the next run as `last_run_at + interval`. If the workflow has never run, the first execution is scheduled immediately.
