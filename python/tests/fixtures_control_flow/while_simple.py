"""Test fixture: Simple while loop with single action."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def increment(value: int) -> int:
    return value + 1


@workflow
class WhileSimpleWorkflow(Workflow):
    """Simple while loop incrementing a counter until limit."""

    async def run(self, limit: int) -> int:
        i = 0
        while i < limit:
            i = await increment(i)
        return i
