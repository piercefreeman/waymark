"""Fixture: a retry attempts count read from a variable is not supported."""

from waymark import RetryPolicy, Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class PolicyRetryAttemptsVariableWorkflow(Workflow):
    """Workflow with a variable attempts count - should fail validation."""

    async def run(self, attempts: int) -> str:
        return await self.run_action(flaky_action(), retry=RetryPolicy(attempts=attempts))
