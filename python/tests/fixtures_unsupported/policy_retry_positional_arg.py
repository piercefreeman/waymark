"""Fixture: a retry policy passed to run_action positionally is rejected, not dropped."""

from waymark import RetryPolicy, Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class PolicyRetryPositionalArgWorkflow(Workflow):
    """Workflow passing the policy as a second positional argument - should fail validation."""

    async def run(self) -> str:
        return await self.run_action(flaky_action(), RetryPolicy(attempts=3))  # type: ignore[misc]
