"""Integration test: instance attribute access in workflow methods.

Issue 4: Instance attribute access not supported in workflow methods
Accessing attributes of instance variables (e.g., self.some_object.attribute)
in workflow method code should work correctly.
"""

from datetime import timedelta

from waymark import RetryPolicy, action, workflow
from waymark.workflow import Workflow


class RetryConfig:
    """Configuration object with attributes."""

    def __init__(self, attempts: int, backoff_seconds: int) -> None:
        self.attempts = attempts
        self.backoff_seconds = backoff_seconds


@action
async def perform_action(max_attempts: int, backoff: int) -> dict:
    """Perform an action with the given configuration values."""
    return {"max_attempts": max_attempts, "backoff": backoff}


@action
async def format_config_result(result: dict) -> dict:
    """Format the result."""
    return {"success": True, "config_used": result}


@workflow
class InstanceAttributeAccessWorkflow(Workflow):
    """Test that instance attribute access works in workflow methods."""

    def __init__(self) -> None:
        super().__init__()
        self.crawl_retry = RetryConfig(attempts=3, backoff_seconds=10)
        self.max_retries = 5

    async def run(self) -> dict:
        """
        Expected: Accessing self.crawl_retry.attempts works correctly.
        Bug: Workflow fails with NoneType exception.
        """
        # Test accessing nested attribute
        result = await perform_action(
            max_attempts=self.crawl_retry.attempts,
            backoff=self.crawl_retry.backoff_seconds,
        )

        return await format_config_result(result=result)
