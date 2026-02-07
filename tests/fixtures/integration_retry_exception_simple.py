"""
Integration test: Simple exception should be caught after retry exhaustion.

Minimal reproduction case where:
1. An action raises ValueError with retry=RetryPolicy(attempts=3)
2. All 3 attempts fail
3. The exception should be caught by `except ValueError:` handler
4. Result is returned AFTER the try/except block (not from inside)

This isolates the issue from any complications with returns inside try blocks.
"""

from waymark import RetryPolicy, action, workflow
from waymark.workflow import Workflow


@action
async def failing_action() -> str:
    """Action that always fails with ValueError."""
    raise ValueError("Always fails")


@action
async def success_action(msg: str) -> str:
    """Action that succeeds."""
    return f"success:{msg}"


@action
async def recovery_action() -> str:
    """Action to run in exception handler."""
    return "recovered"


@workflow
class RetryExceptionSimpleWorkflow(Workflow):
    """
    Simple workflow that catches ValueError after retry exhaustion.
    """

    async def run(self) -> str:
        result = "initial"
        try:
            result = await self.run_action(
                failing_action(),
                retry=RetryPolicy(attempts=3),
            )
        except ValueError:
            result = await recovery_action()
        return result
