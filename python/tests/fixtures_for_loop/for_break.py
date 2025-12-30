"""Test fixture: For loop with break statement."""

from rappel import action, workflow
from rappel.workflow import Workflow


@action
async def check_item(value: int) -> bool:
    """Check if item matches condition."""
    return value == 5


@action
async def process_found(value: int) -> str:
    """Process the found item."""
    return f"found:{value}"


@workflow
class ForBreakWorkflow(Workflow):
    """Workflow that breaks out of a for loop."""

    async def run(self, items: list[int]) -> str:
        result = "not found"
        for item in items:
            found = await check_item(value=item)
            if found:
                result = await process_found(value=item)
                break
        return result
