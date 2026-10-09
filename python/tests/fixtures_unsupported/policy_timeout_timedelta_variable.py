"""Fixture: a timedelta timeout built from a variable is not supported."""

from datetime import timedelta

from waymark import Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class PolicyTimeoutTimedeltaVariableWorkflow(Workflow):
    """Workflow with timeout=timedelta(seconds=<variable>) - should fail validation."""

    async def run(self, seconds: int) -> str:
        return await self.run_action(flaky_action(), timeout=timedelta(seconds=seconds))
