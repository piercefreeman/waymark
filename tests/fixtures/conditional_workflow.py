"""Conditional workflow fixture - tests if/elif/else branching."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def get_score(tier: str) -> int:
    """Return a score based on tier."""
    if tier == "high":
        return 100
    elif tier == "medium":
        return 50
    else:
        return 10


@action
async def evaluate_high(score: int) -> str:
    """Handle high scores (>= 75)."""
    return f"excellent:{score}"


@action
async def evaluate_medium(score: int) -> str:
    """Handle medium scores (25-74)."""
    return f"good:{score}"


@action
async def evaluate_low(score: int) -> str:
    """Handle low scores (< 25)."""
    return f"needs_work:{score}"


@workflow
class ConditionalWorkflow(Workflow):
    """Workflow with if/elif/else branching."""

    async def run(self, tier: str = "medium") -> str:
        score = await get_score(tier=tier)

        if score >= 75:
            result = await evaluate_high(score=score)
        elif score >= 25:
            result = await evaluate_medium(score=score)
        else:
            result = await evaluate_low(score=score)

        return result
