"""Fixture: a zero timeout is not supported."""

from waymark import Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class PolicyTimeoutZeroWorkflow(Workflow):
    """Workflow with timeout=0 - should fail validation."""

    async def run(self) -> str:
        return await self.run_action(flaky_action(), timeout=0)
