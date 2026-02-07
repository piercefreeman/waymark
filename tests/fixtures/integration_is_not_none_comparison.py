"""Integration test: is not None comparisons in workflow code.

Issue 5: is not None comparisons not supported
Using 'is not None' comparisons in workflow code should work correctly,
not raise UnsupportedPatternError.
"""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def maybe_get_value(should_return: bool) -> str | None:
    """Return a value or None based on the flag."""
    if should_return:
        return "found_value"
    return None


@action
async def process_value(value: str) -> str:
    """Process a non-None value."""
    return f"processed_{value}"


@action
async def handle_none() -> str:
    """Handle the None case."""
    return "no_value"


@action
async def format_result(result: str, had_value: bool) -> dict:
    """Format the final result."""
    return {"result": result, "had_value": had_value}


@workflow
class IsNotNoneComparisonWorkflow(Workflow):
    """Test that 'is not None' comparisons work in workflow code."""

    async def run(self, should_return: bool = True) -> dict:
        """
        Expected: 'is not None' comparison works correctly.
        Bug: UnsupportedPatternError for Compare expression type.
        """
        value = await maybe_get_value(should_return=should_return)

        if value is not None:
            result = await process_value(value=value)
            had_value = True
        else:
            result = await handle_none()
            had_value = False

        return await format_result(result=result, had_value=had_value)
