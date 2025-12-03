"""Integration test for durable sleep functionality.

This tests that asyncio.sleep() is properly converted to a scheduler-managed
sleep node that can survive worker restarts.
"""
import asyncio
from datetime import datetime

from rappel import action, workflow
from rappel.workflow import Workflow


@action
async def get_timestamp() -> str:
    """Get current timestamp as string."""
    return datetime.now().isoformat()


@action
async def format_sleep_result(started: str, resumed: str) -> str:
    """Format the sleep result showing timestamps."""
    start_dt = datetime.fromisoformat(started)
    end_dt = datetime.fromisoformat(resumed)
    duration = (end_dt - start_dt).total_seconds()
    return f"slept:{duration:.1f}s"


@workflow
class SleepWorkflow(Workflow):
    """Workflow that uses durable sleep."""

    async def run(self) -> str:
        started = await get_timestamp()

        # Durable sleep - this should be handled by the scheduler, not the worker
        await asyncio.sleep(1)

        resumed = await get_timestamp()

        result = await format_sleep_result(started, resumed)
        return result
