"""Integration test for complex workflow logic.

This tests workflows with:
- Intermediate variables and computations
- Complex conditional expressions
- Variable reassignment
- Multiple action results combined
"""
from rappel import action, workflow
from rappel.workflow import Workflow


@action
async def fetch_base_value(key: str) -> int:
    """Fetch a base value based on key."""
    values = {"alpha": 10, "beta": 25, "gamma": 50, "delta": 100}
    return values.get(key, 0)


@action
async def compute_multiplier(value: int) -> float:
    """Compute a multiplier based on value range."""
    if value < 20:
        return 1.5
    elif value < 60:
        return 2.0
    else:
        return 3.0


@action
async def apply_transform(value: int, multiplier: float, offset: int) -> int:
    """Apply a transform: (value * multiplier) + offset."""
    return int(value * multiplier) + offset


@action
async def categorize_result(value: int) -> str:
    """Categorize the final result."""
    if value < 30:
        return "small"
    elif value < 100:
        return "medium"
    elif value < 200:
        return "large"
    else:
        return "huge"


@action
async def format_output(
    original: int, transformed: int, category: str, bonus_applied: bool
) -> str:
    """Format the final output string."""
    bonus_str = "+bonus" if bonus_applied else ""
    return f"{category}:{original}->{transformed}{bonus_str}"


@workflow
class ComplexLogicWorkflow(Workflow):
    """Workflow with complex intermediate logic."""

    async def run(self, key: str, apply_bonus: bool) -> str:
        # Fetch and store in intermediate variable
        base = await fetch_base_value(key)

        # Compute derived value
        mult = await compute_multiplier(base)

        # Calculate offset based on conditions
        offset = 0
        if base > 30:
            offset = 10
        if apply_bonus:
            offset = offset + 5

        # Apply transformation
        result = await apply_transform(base, mult, offset)

        # Categorize
        category = await categorize_result(result)

        # Format and return
        output = await format_output(base, result, category, apply_bonus)
        return output
