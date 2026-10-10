"""Fixture: a retry filter entry that is a class instead of a string is not supported."""

from waymark import ActionTimeout, RetryPolicy, Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class RetryExceptionClassWorkflow(Workflow):
    """Workflow passing a class in exception_types - should fail validation."""

    async def run(self) -> str:
        return await self.run_action(
            flaky_action(),
            retry=RetryPolicy(
                attempts=2,
                # The wrong type is the point: the builder must reject it.
                exception_types=[ActionTimeout],  # type: ignore[invalid-argument-type]
            ),
            timeout=1,
        )
