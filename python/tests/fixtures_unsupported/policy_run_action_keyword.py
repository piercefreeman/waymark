"""Fixture: a run_action keyword other than retry and timeout is not supported."""

from waymark import Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class PolicyRunActionKeywordWorkflow(Workflow):
    """Workflow passing retries= to run_action - should fail validation."""

    async def run(self) -> str:
        # The unknown keyword is the point: the builder must reject it.
        return await self.run_action(flaky_action(), retries=3)  # type: ignore[unknown-argument]
