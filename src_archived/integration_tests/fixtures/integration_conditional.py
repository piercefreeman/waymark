"""Integration test for conditional branching (if/elif/else).

This tests that if/elif/else chains are properly converted to guarded nodes
and that only the correct branch executes.
"""
from rappel import action, workflow
from rappel.workflow import Workflow


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


@action
async def get_value(tier: str) -> int:
    """Return a value based on tier."""
    if tier == "high":
        return 100
    elif tier == "medium":
        return 50
    else:
        return 10


@workflow
class ConditionalWorkflow(Workflow):
    """Workflow with if/elif/else branching."""

    async def run(self, tier: str) -> str:
        value = await get_value(tier)

        if value >= 75:
            result = await evaluate_high(value)
            branch = "high"
        elif value >= 25:
            result = await evaluate_medium(value)
            branch = "medium"
        else:
            result = await evaluate_low(value)
            branch = "low"

        return f"{branch}:{result}"
