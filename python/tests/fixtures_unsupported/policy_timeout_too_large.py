"""Fixture: a timeout above the 100-year cap is rejected."""

from waymark import Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class PolicyTimeoutTooLargeWorkflow(Workflow):
    """Workflow with timeout=1e20 - should fail validation."""

    async def run(self) -> str:
        return await self.run_action(flaky_action(), timeout=1e20)
