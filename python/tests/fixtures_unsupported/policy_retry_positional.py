"""Fixture: a retry policy built with positional arguments is not supported."""

from waymark import RetryPolicy, Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class PolicyRetryPositionalWorkflow(Workflow):
    """Workflow building RetryPolicy positionally - should fail validation."""

    async def run(self) -> str:
        return await self.run_action(flaky_action(), retry=RetryPolicy(3))
