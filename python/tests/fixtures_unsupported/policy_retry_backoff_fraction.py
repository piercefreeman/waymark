"""Fixture: a fractional backoff is not supported."""

from waymark import RetryPolicy, Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class PolicyRetryBackoffFractionWorkflow(Workflow):
    """Workflow with backoff_seconds=0.5 - should fail validation."""

    async def run(self) -> str:
        return await self.run_action(
            flaky_action(), retry=RetryPolicy(attempts=3, backoff_seconds=0.5)
        )
