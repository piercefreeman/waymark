"""Test fixture: if branch with multiple action calls (triggers body wrapping)."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action(name="if_step_one")
async def if_step_one(value: int) -> int:
    """First step of processing."""
    return value + 1


@action(name="if_step_two")
async def if_step_two(value: int) -> int:
    """Second step of processing."""
    return value * 2


@workflow
class IfMultipleCallsWorkflow(Workflow):
    """Workflow with if branch containing multiple action calls.

    This triggers body wrapping - multiple calls get wrapped into a synthetic function.
    """

    async def run(self, value: int) -> int:
        if value > 0:
            # Multiple calls in if body - should trigger wrapping
            a = await if_step_one(value=value)
            b = await if_step_two(value=a)
            return b
        else:
            return 0
