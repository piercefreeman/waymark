"""Fixture: a retry policy built into a local variable is not supported.

A module-level policy compiles from the value the global holds, but a local
is a runtime value the compiler cannot read, so this must stay rejected.
"""

from waymark import RetryPolicy, Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class PolicyRetryVariableWorkflow(Workflow):
    """Workflow passing a local variable as the retry policy - should fail validation."""

    async def run(self) -> str:
        policy = RetryPolicy(attempts=2)
        return await self.run_action(flaky_action(), retry=policy)
