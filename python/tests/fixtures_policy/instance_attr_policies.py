"""Test fixture: Instance attribute policy references."""

from waymark import action, workflow
from waymark.workflow import RetryPolicy, Workflow


@action
async def action_with_instance_retry(value: str) -> str:
    """Action using instance-level retry policy."""
    return f"done({value})"


@action
async def action_with_instance_timeout(value: str) -> str:
    """Action using instance-level timeout policy."""
    return f"done({value})"


@action
async def action_with_both_policies(value: str) -> str:
    """Action using both instance-level retry and timeout."""
    return f"done({value})"


@workflow
class InstanceAttrPoliciesWorkflow(Workflow):
    """Workflow with policies stored as instance attributes."""

    def __init__(self) -> None:
        super().__init__()
        # Store policies as instance attributes
        self.retry_policy = RetryPolicy(attempts=3, backoff_seconds=10)
        self.timeout_value = 120
        self.fast_retry = RetryPolicy(attempts=2)

    async def run(self, value: str) -> str:
        # Use instance attribute retry policy
        a = await self.run_action(action_with_instance_retry(value=value), retry=self.retry_policy)

        # Use instance attribute timeout
        b = await self.run_action(action_with_instance_timeout(value=a), timeout=self.timeout_value)

        # Use both policies from instance attributes
        c = await self.run_action(
            action_with_both_policies(value=b),
            retry=self.fast_retry,
            timeout=self.timeout_value,
        )

        return c
