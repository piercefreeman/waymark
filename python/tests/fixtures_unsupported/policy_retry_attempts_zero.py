"""Fixture: a retry policy with zero attempts is not supported."""

from waymark import RetryPolicy, Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class PolicyRetryAttemptsZeroWorkflow(Workflow):
    """Workflow with attempts=0 - should fail validation."""

    async def run(self) -> str:
        return await self.run_action(flaky_action(), retry=RetryPolicy(attempts=0))
