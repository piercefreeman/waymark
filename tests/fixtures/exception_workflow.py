"""Exception workflow fixture - tests try/except handling."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def get_initial_value() -> int:
    """Get an initial value."""
    return 42


@action
async def risky_operation(value: int) -> int:
    """An operation that might fail."""
    if value > 100:
        raise ValueError("value too large: " + str(value))
    return value * 2


@action
async def handle_error(message: str) -> str:
    """Handle an error."""
    return "handled:" + message


@action
async def format_success(result: int) -> str:
    """Format a success message."""
    return "success:" + str(result)


@workflow
class ExceptionWorkflow(Workflow):
    """Workflow with try/except handling.

    The IR builder will automatically transform this:
    - Wrap the multi-call try body into a synthetic function
    - The try block then has a single call to that function
    - Both actions remain protected by exception handling
    """

    async def run(self) -> str:
        try:
            value = await get_initial_value()
            result = await risky_operation(value=value)
            return await format_success(result=result)
        except ValueError:
            error_result = await handle_error(message="fallback")
            return error_result
