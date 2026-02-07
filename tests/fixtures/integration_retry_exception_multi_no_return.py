"""
Integration test: Multiple actions in try WITHOUT return inside try.

Tests if the issue is specifically the return inside try, or just having
multiple actions in the try block.
"""

from waymark import RetryPolicy, action, workflow
from waymark.workflow import Workflow


@action
async def failing_action(user_id: str) -> dict:
    """Action that always fails with ValueError."""
    raise ValueError(f"Unable to find user: {user_id}")


@action
async def process_result(data: dict) -> str:
    """Process the result."""
    return f"processed:{data}"


@workflow
class RetryExceptionMultiNoReturnWorkflow(Workflow):
    """
    Workflow with multiple actions in try block but NO return inside try.
    """

    async def run(self, user_id: str) -> str:
        result = "initial"
        try:
            metadata = await self.run_action(
                failing_action(user_id=user_id),
                retry=RetryPolicy(attempts=3),
            )
            result = await process_result(data=metadata)
        except ValueError:
            result = "caught_error"
        return result
