"""Fixture: a negative backoff is rejected for its range, not as a non-literal."""

from waymark import RetryPolicy, Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class PolicyRetryBackoffNegativeWorkflow(Workflow):
    """Workflow with backoff_seconds=-1 - should fail validation on the range."""

    async def run(self) -> str:
        return await self.run_action(
            flaky_action(), retry=RetryPolicy(attempts=3, backoff_seconds=-1)
        )
