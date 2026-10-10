"""Fixture: a `self.` policy assigned twice in __init__ is rejected, not last-wins."""

from waymark import RetryPolicy, Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class PolicyRetrySelfAssignedTwiceWorkflow(Workflow):
    """Workflow assigning `self.policy` twice - should fail validation."""

    def __init__(self) -> None:
        super().__init__()
        self.policy = RetryPolicy(attempts=1)
        self.policy = RetryPolicy(attempts=9)

    async def run(self) -> str:
        return await self.run_action(flaky_action(), retry=self.policy)
