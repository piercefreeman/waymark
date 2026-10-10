"""Fixture: a retry policy held in a variable is not supported."""

from waymark import RetryPolicy, Workflow, action, workflow

POLICY = RetryPolicy(attempts=2)


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class PolicyRetryVariableWorkflow(Workflow):
    """Workflow passing a variable as the retry policy - should fail validation."""

    async def run(self) -> str:
        return await self.run_action(flaky_action(), retry=POLICY)
