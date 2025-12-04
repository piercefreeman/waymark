"""Test fixture: for loop with multiple action calls (triggers body wrapping)."""

from rappel import action, workflow
from rappel.workflow import Workflow


@action(name="for_step_one")
async def for_step_one(item: str) -> str:
    """First step of processing."""
    return f"step1({item})"


@action(name="for_step_two")
async def for_step_two(value: str) -> str:
    """Second step of processing."""
    return f"step2({value})"


@workflow
class ForMultipleCallsWorkflow(Workflow):
    """Workflow with for loop containing multiple action calls.

    This triggers body wrapping - multiple calls get wrapped into a synthetic function.
    """

    async def run(self, items: list[str]) -> list[str]:
        results = []
        for item in items:
            # Multiple calls in for body - should trigger wrapping
            a = await for_step_one(item=item)
            b = await for_step_two(value=a)
            results.append(b)
        return results
