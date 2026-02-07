"""Fixture: for loop with counter accumulation."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action(name="counter_accum_check_valid")
async def check_valid(item: str) -> bool:
    """Check if item is valid (length > 2)."""
    return len(item) > 2


@workflow
class ForCounterAccumulatorWorkflow(Workflow):
    """For loop that counts valid items."""

    async def run(self, items: list) -> int:
        count = 0
        for item in items:
            is_valid = await check_valid(item)
            if is_valid:
                count = count + 1
        return count
