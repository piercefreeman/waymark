"""Fixture: conditional with accumulator pattern.

This tests that out-of-scope variable modifications in conditionals
are properly handled when the body is wrapped into a synthetic function.
"""

from rappel import action, workflow
from rappel.workflow import Workflow


@action(name="if_accum_process_high")
async def process_high(value: int) -> str:
    """Process high value."""
    return f"high:{value}"


@action(name="if_accum_process_low")
async def process_low(value: int) -> str:
    """Process low value."""
    return f"low:{value}"


@action(name="if_accum_finalize")
async def finalize(result: str) -> str:
    """Finalize the result."""
    return f"[{result}]"


@workflow
class IfWithAccumulatorWorkflow(Workflow):
    """Conditional that modifies an out-of-scope variable with multiple actions."""

    async def run(self, value: int) -> str:
        results = []
        if value > 50:
            # Multiple actions in branch -> wrapped into synthetic function
            x = await process_high(value)
            y = await finalize(x)
            results.append(y)
        else:
            # Single action in branch -> not wrapped
            z = await process_low(value)
            results.append(z)
        if results:
            return results[0]
        return "empty"
