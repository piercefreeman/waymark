"""Integration timeout workflow fixture for Rust timeout enforcement."""

import asyncio
from datetime import timedelta

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def slow_action() -> str:
    """Sleep longer than policy timeout to force Rust-side timeout handling."""
    await asyncio.sleep(2)
    return "late"


@workflow
class TimeoutWorkflow(Workflow):
    """Workflow expected to fail with ActionTimeout in Rust runtime."""

    async def run(self) -> str:
        return await self.run_action(slow_action(), timeout=timedelta(seconds=1))
