"""Fixture: a timeout read from a variable is not supported."""

from waymark import Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class PolicyTimeoutVariableWorkflow(Workflow):
    """Workflow with a variable timeout - should fail validation."""

    async def run(self, seconds: int) -> str:
        return await self.run_action(flaky_action(), timeout=seconds)
