"""Fixture: a timeout that rounds to zero at nanosecond precision is rejected."""

from waymark import Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class PolicyTimeoutTooSmallWorkflow(Workflow):
    """Workflow with timeout=1e-10 - should fail validation."""

    async def run(self) -> str:
        return await self.run_action(flaky_action(), timeout=1e-10)
