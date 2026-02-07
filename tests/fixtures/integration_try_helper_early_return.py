"""Integration test for try/except with helper functions containing early returns.

This tests a bug where a helper function with early-return branches is called
inside a try block, and the exception handler incorrectly depends on the
unreachable early-return branches.

Pattern that causes the bug:
    async def helper(value):
        if condition1:
            await error_action1()
            return None  # early return 1 (never executed when condition1 is false)
        result = await main_action()  # may throw exception
        return result

    async def run():
        try:
            result = await helper(value)  # main_action throws
            final = await process_result(result)
        except SomeError:
            # BUG: This handler incorrectly waits for error_action1 to complete
            # even though it was never executed (condition1 was false)
            await handle_error()
        return final
"""

from waymark import action, workflow
from waymark.workflow import Workflow


class ProcessingError(Exception):
    """Raised when processing fails."""

    pass


@action
async def check_should_skip(value: str) -> bool:
    """Check if we should skip processing."""
    # Simulate: value "skip" causes skip, others proceed
    return value == "skip"


@action
async def do_skip_action() -> str:
    """Early return action when skipping."""
    return "skipped"


@action
async def process_value(value: str) -> str:
    """Process the value - may throw exception."""
    if value == "error":
        raise ProcessingError("Processing failed")
    return f"processed:{value}"


@action
async def format_result(result: str) -> str:
    """Format the successful result."""
    return f"success:{result}"


@action
async def handle_processing_error(message: str) -> str:
    """Handle the processing error."""
    return f"handled:{message}"


@workflow
class TryHelperEarlyReturnWorkflow(Workflow):
    """Workflow that tests try/except with helper early returns.

    The helper function `do_processing` has two paths:
    1. Early return if should_skip (do_skip_action)
    2. Normal path that calls process_value (may throw)

    When process_value throws, the exception handler should NOT wait for
    do_skip_action (which was never executed because should_skip was false).
    """

    async def do_processing(self, value: str) -> str:
        """Helper with early return and potentially throwing action."""
        should_skip = await self.run_action(check_should_skip(value))
        if should_skip:
            result = await self.run_action(do_skip_action())
            return result

        # This action may throw ProcessingError
        result = await self.run_action(process_value(value))
        return result

    async def run(self, value: str = "normal") -> str:
        """Main workflow entry point with try/except around helper call."""
        try:
            # Call the helper function (will be inlined)
            result = await self.do_processing(value)
            # This continuation should only execute on success
            final = await self.run_action(format_result(result))
        except ProcessingError:
            # BUG: This handler incorrectly waits for do_skip_action
            # even when should_skip was false (do_skip_action never ran)
            final = await self.run_action(
                handle_processing_error("caught processing error")
            )
        return final
