"""Test fixture: For loop with continue statement."""

from rappel import action, workflow
from rappel.workflow import Workflow


@action
async def should_skip(value: int) -> bool:
    """Check if item should be skipped."""
    return value < 0


@action
async def process_item(value: int) -> int:
    """Process an item."""
    return value * 2


@workflow
class ForContinueWorkflow(Workflow):
    """Workflow that uses continue to skip iterations."""

    async def run(self, items: list[int]) -> int:
        total = 0
        for item in items:
            skip = await should_skip(value=item)
            if skip:
                continue
            result = await process_item(value=item)
            total = total + result
        return total
