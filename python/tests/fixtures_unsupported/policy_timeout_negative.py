"""Fixture: a negative timeout is rejected with the range message."""

from waymark import Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class PolicyTimeoutNegativeWorkflow(Workflow):
    """Workflow with timeout=-5 - should fail validation."""

    async def run(self) -> str:
        return await self.run_action(flaky_action(), timeout=-5)
