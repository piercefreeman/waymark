"""Fixture: a `self.` retry policy with no literal assigned in __init__ is not supported."""

from waymark import RetryPolicy, Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class PolicyRetrySelfUnassignedWorkflow(Workflow):
    """Workflow reading its retry policy from an unassigned attribute - should fail validation."""

    policy: RetryPolicy

    async def run(self) -> str:
        return await self.run_action(flaky_action(), retry=self.policy)
