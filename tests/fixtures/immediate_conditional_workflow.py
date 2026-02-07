"""Immediate conditional workflow fixture - tests if/elif/else with guards on input values.

This workflow tests the case where conditional branches occur immediately at workflow
start, with guards that depend on input values (not action results).
"""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def evaluate_high(value: int) -> str:
    """Handle high values (>= 75)."""
    return f"high:{value}"


@action
async def evaluate_medium(value: int) -> str:
    """Handle medium values (25-74)."""
    return f"medium:{value}"


@action
async def evaluate_low(value: int) -> str:
    """Handle low values (< 25)."""
    return f"low:{value}"


@workflow
class ImmediateConditionalWorkflow(Workflow):
    """Workflow with immediate if/elif/else branching based on input value."""

    async def run(self, value: int = 50) -> str:
        # Guards depend directly on input, not an action result
        if value >= 75:
            result = await evaluate_high(value=value)
        elif value >= 25:
            result = await evaluate_medium(value=value)
        else:
            result = await evaluate_low(value=value)

        return result
