"""Fixture: a float literal that parses as infinity is not a timeout."""

from waymark import Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class PolicyTimeoutInfiniteWorkflow(Workflow):
    """Workflow with timeout=1e400 (infinity) - should fail validation."""

    async def run(self) -> str:
        return await self.run_action(flaky_action(), timeout=1e400)
