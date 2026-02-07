"""Test fixture: try body with multiple action calls (triggers body wrapping)."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def risky_step_one(value: str) -> str:
    """First risky step."""
    return f"step1({value})"


@action
async def risky_step_two(value: str) -> str:
    """Second risky step."""
    return f"step2({value})"


@workflow
class TryMultipleCallsWorkflow(Workflow):
    """Workflow with try body containing multiple action calls.

    This triggers body wrapping - multiple calls get wrapped into a synthetic function.
    """

    async def run(self, value: str) -> str:
        try:
            # Multiple calls in try body - should trigger wrapping
            a = await risky_step_one(value=value)
            b = await risky_step_two(value=a)
            return b
        except ValueError:
            return "error"
