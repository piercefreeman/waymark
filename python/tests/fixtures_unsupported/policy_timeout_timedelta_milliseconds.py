"""Fixture: a timedelta timeout built from milliseconds is not supported."""

from datetime import timedelta

from waymark import Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class PolicyTimeoutTimedeltaMillisecondsWorkflow(Workflow):
    """Workflow with timeout=timedelta(milliseconds=500) - should fail validation."""

    async def run(self) -> str:
        return await self.run_action(flaky_action(), timeout=timedelta(milliseconds=500))
