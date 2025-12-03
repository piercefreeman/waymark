"""Exception workflow fixture - tests try/except handling."""

from rappel import action, workflow
from rappel.workflow import Workflow


@action
async def get_initial_value() -> int:
    """Get an initial value."""
    return 42


@action
async def risky_operation(value: int) -> int:
    """An operation that might fail."""
    if value > 100:
        raise ValueError(f"value too large: {value}")
    return value * 2


@action
async def handle_error(message: str) -> str:
    """Handle an error."""
    return f"handled:{message}"


@workflow
class ExceptionWorkflow(Workflow):
    """Workflow with try/except handling.

    The IR builder will automatically transform this:
    - Hoist get_initial_value() out of the try block
    - Keep only risky_operation() in the try block
    - Move the return after the try/except
    """

    async def run(self) -> str:
        try:
            value = await get_initial_value()
            result = await risky_operation(value=value)
            return f"success:{result}"
        except ValueError:
            error_result = await handle_error(message="fallback")
            return error_result
