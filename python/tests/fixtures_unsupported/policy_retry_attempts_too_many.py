"""Fixture: `attempts` above the IR's uint32 retry budget is rejected, not crashed on."""

from waymark import Workflow, action, workflow
from waymark.workflow import RetryPolicy


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class PolicyRetryAttemptsTooManyWorkflow(Workflow):
    """Workflow with attempts=2**32 + 1 - should fail validation."""

    async def run(self) -> str:
        return await self.run_action(flaky_action(), retry=RetryPolicy(attempts=4294967297))
