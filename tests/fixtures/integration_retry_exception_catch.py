"""
Integration test: Exception should be caught after retry exhaustion.

This reproduces a bug where:
1. An action raises ValueError with retry=RetryPolicy(attempts=N)
2. All N attempts fail
3. The exception should be caught by `except ValueError:` handler
4. BUG: Workflow stalls instead of exception being caught

Expected behavior:
- Action fails N times
- Exception propagates to try/except handler
- Handler executes and workflow completes

Bug behavior:
- Action fails N times
- Workflow stalls with "workflow stalled without pending work"
"""

from waymark import RetryPolicy, action, workflow
from waymark.workflow import Workflow


@action
async def get_task_metadata(user_id: str) -> dict:
    """Simulates an action that always fails with ValueError."""
    raise ValueError(f"Unable to find a valid auth session for user: {user_id}")


@action
async def process_result(data: dict) -> str:
    """Process the result data."""
    return f"processed:{data}"


@workflow
class RetryExceptionCatchWorkflow(Workflow):
    """
    Workflow that catches ValueError after retry exhaustion.

    This workflow should:
    1. Try to run get_task_metadata with 3 retries
    2. All 3 attempts raise ValueError
    3. Exception is caught by `except ValueError:`
    4. Return "caught_error"
    """

    async def run(self, user_id: str) -> str:
        try:
            proxy_metadata = await self.run_action(
                get_task_metadata(user_id=user_id),
                retry=RetryPolicy(attempts=3),
            )
            result = await process_result(data=proxy_metadata)
            return result
        except ValueError:
            # Normal error if the user didn't have a valid auth session
            return "caught_error"
