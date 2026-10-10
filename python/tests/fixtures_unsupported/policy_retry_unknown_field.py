"""Fixture: a retry policy with a field RetryPolicy does not have is not supported."""

from waymark import RetryPolicy, Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class PolicyRetryUnknownFieldWorkflow(Workflow):
    """Workflow passing max_retries to RetryPolicy - should fail validation."""

    async def run(self) -> str:
        return await self.run_action(
            flaky_action(),
            # The unknown field is the point: the builder must reject it.
            retry=RetryPolicy(max_retries=3),  # type: ignore[unknown-argument]
        )
