"""
Integration test: Multiple actions in try with return inside try block.

This tests the case where:
1. There are multiple actions in the try block
2. There's a return statement INSIDE the try block
3. The first action fails after retry exhaustion
4. The exception should be caught by the except handler
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
class RetryExceptionMultiActionWorkflow(Workflow):
    """
    Workflow with multiple actions in try block and return inside try.

    This reproduces the bug where:
    1. First action fails after retries
    2. Workflow stalls instead of catching the exception
    """

    async def run(self, user_id: str) -> str:
        try:
            metadata = await self.run_action(
                failing_action(user_id=user_id),
                retry=RetryPolicy(attempts=3),
            )
            result = await process_result(data=metadata)
            return result
        except ValueError:
            return "caught_error"
