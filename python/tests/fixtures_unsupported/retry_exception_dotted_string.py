"""Fixture: a dotted class name in a retry filter is not supported."""

from waymark import RetryPolicy, Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class RetryExceptionDottedStringWorkflow(Workflow):
    """Workflow passing a module path in exception_types - should fail validation."""

    async def run(self) -> str:
        return await self.run_action(
            flaky_action(),
            retry=RetryPolicy(attempts=2, exception_types=["httpx.ConnectError"]),
        )
