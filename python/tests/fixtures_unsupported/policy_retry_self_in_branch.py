"""Fixture: a `self.` policy assigned under a branch in __init__ is rejected."""

from waymark import RetryPolicy, Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class PolicyRetrySelfInBranchWorkflow(Workflow):
    """Workflow choosing `self.policy` in an `if` - should fail validation."""

    def __init__(self, fast: bool = False) -> None:
        super().__init__()
        if fast:
            self.policy = RetryPolicy(attempts=1)
        else:
            self.policy = RetryPolicy(attempts=9)

    async def run(self) -> str:
        return await self.run_action(flaky_action(), retry=self.policy)
