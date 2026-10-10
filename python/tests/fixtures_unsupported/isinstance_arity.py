"""Fixture: isinstance() with one argument is rejected with the arity message."""

from waymark import Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class IsinstanceArityWorkflow(Workflow):
    """Workflow calling isinstance(error) - should fail validation."""

    async def run(self) -> str:
        try:
            return await flaky_action()
        except Exception as error:
            if isinstance(error):  # type: ignore[missing-argument] - the builder must reject it
                return "one argument"
            return "unreachable"
