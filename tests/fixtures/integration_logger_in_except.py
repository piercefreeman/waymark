"""Integration test: action calls in exception handlers.

Issue 6: LOGGER calls not supported in workflow exception handlers
Direct LOGGER.info() or similar calls inside workflow exception handlers cause
IR compilation to fail. This test verifies that action calls work in except blocks.

Note: The workaround is to use an action for logging instead of direct LOGGER calls.
This test verifies that the workaround (using actions in except handlers) works correctly.
"""

from datetime import timedelta

from waymark import RetryPolicy, action, workflow
from waymark.workflow import Workflow


class FetchError(Exception):
    """Custom fetch error."""

    pass


@action
async def risky_fetch(should_fail: bool) -> str:
    """Perform a risky fetch that may fail."""
    if should_fail:
        raise FetchError("Fetch failed")
    return "fetch_success"


@action
async def log_action(message: str) -> str:
    """Log a message via an action (workaround for LOGGER calls)."""
    return message


@action
async def format_log_result(logs: list[str], success: bool) -> dict:
    """Format the result with logs."""
    return {"logs": logs, "success": success}


@workflow
class LoggerInExceptWorkflow(Workflow):
    """Test that action calls work in exception handlers."""

    async def run(self, should_fail: bool = True) -> dict:
        """
        Expected: Action calls in except handlers work correctly.
        Bug: IR compilation fails when processing exception handler.
        """
        logs: list[str] = []
        success = False

        try:
            await self.run_action(
                risky_fetch(should_fail=should_fail),
                retry=RetryPolicy(attempts=1),
                timeout=timedelta(seconds=5),
            )
            success = True
            log = await log_action(message="Fetch succeeded")
            logs.append(log)
        except FetchError:
            # This action call should work in the except handler
            log = await log_action(message="FetchError caught")
            logs.append(log)
        except Exception:
            # This action call should also work
            log = await log_action(message="Generic exception caught")
            logs.append(log)

        return await format_log_result(logs=logs, success=success)
