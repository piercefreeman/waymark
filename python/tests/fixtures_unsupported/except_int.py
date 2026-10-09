"""Fixture: `except int:` names a class that is not an exception, so it is rejected."""

from waymark import Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class ExceptIntWorkflow(Workflow):
    """Workflow listing `except int:` - would compile to a class that never matches."""

    async def run(self) -> str:
        try:
            result = await flaky_action()
        except int:  # type: ignore[invalid-exception-caught] - the builder must reject it
            result = "handled"
        return result
