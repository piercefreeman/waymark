"""Fixture: a timedelta(...) timeout outside timedelta's range is rejected, not crashed on."""

from datetime import timedelta

from waymark import Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class PolicyTimeoutTimedeltaOverflowWorkflow(Workflow):
    """Workflow with timeout=timedelta(days=1e10) - should fail validation."""

    async def run(self) -> str:
        return await self.run_action(flaky_action(), timeout=timedelta(days=1e10))
