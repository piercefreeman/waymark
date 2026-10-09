"""Fixture: a float `attempts`, even a whole one, is not an integer literal."""

from waymark import Workflow, action, workflow
from waymark.workflow import RetryPolicy


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class PolicyRetryAttemptsFloatWorkflow(Workflow):
    """Workflow with attempts=3.0 - should fail validation."""

    async def run(self) -> str:
        return await self.run_action(
            flaky_action(),
            retry=RetryPolicy(attempts=3.0),  # type: ignore[invalid-argument-type] - rejected on purpose
        )
