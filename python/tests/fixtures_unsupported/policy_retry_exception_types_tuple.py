"""Fixture: a retry filter that is not a list literal is not supported."""

from waymark import RetryPolicy, Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class PolicyRetryExceptionTypesTupleWorkflow(Workflow):
    """Workflow passing a tuple as exception_types - should fail validation."""

    async def run(self) -> str:
        return await self.run_action(
            flaky_action(),
            retry=RetryPolicy(
                attempts=2,
                # The wrong type is the point: the builder must reject it.
                exception_types=("ValueError",),  # type: ignore[invalid-argument-type]
            ),
        )
