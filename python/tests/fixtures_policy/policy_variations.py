"""Test fixture: Various policy configurations for testing IR builder coverage."""

from datetime import timedelta

from rappel import action, workflow
from rappel.workflow import RetryPolicy, Workflow


@action
async def action_with_timeout_int(value: str) -> str:
    """Action with integer timeout."""
    return f"done({value})"


@action
async def action_with_timeout_minutes(value: str) -> str:
    """Action with timedelta minutes."""
    return f"done({value})"


@action
async def action_with_retry_backoff(value: str) -> str:
    """Action with retry backoff."""
    return f"done({value})"


@action
async def action_with_retry_exceptions(value: str) -> str:
    """Action with retry exception types."""
    return f"done({value})"


@action
async def action_with_timeout_hours(value: str) -> str:
    """Action with timedelta hours."""
    return f"done({value})"


@action
async def action_with_timeout_days(value: str) -> str:
    """Action with timedelta days."""
    return f"done({value})"


@workflow
class PolicyVariationsWorkflow(Workflow):
    """Workflow with various policy configurations."""

    async def run(self, value: str) -> str:
        # Test timeout with direct integer (not timedelta)
        a = await self.run_action(action_with_timeout_int(value=value), timeout=60)

        # Test timeout with timedelta minutes
        b = await self.run_action(
            action_with_timeout_minutes(value=a), timeout=timedelta(minutes=2)
        )

        # Test retry with backoff_seconds
        c = await self.run_action(
            action_with_retry_backoff(value=b),
            retry=RetryPolicy(attempts=3, backoff_seconds=5),
        )

        # Test retry with exception_types
        d = await self.run_action(
            action_with_retry_exceptions(value=c),
            retry=RetryPolicy(attempts=2, exception_types=["ValueError", "KeyError"]),
        )

        # Test timeout with timedelta hours
        e = await self.run_action(action_with_timeout_hours(value=d), timeout=timedelta(hours=1))

        # Test timeout with timedelta days
        f = await self.run_action(action_with_timeout_days(value=e), timeout=timedelta(days=1))

        return f
