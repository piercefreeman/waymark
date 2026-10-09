"""Fixture: an except clause with an empty tuple is not supported."""

from waymark import Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class ExceptEmptyTupleWorkflow(Workflow):
    """Workflow with `except ():` - should fail validation."""

    async def run(self) -> str:
        try:
            result = await flaky_action()
        except ():  # noqa: B029 - the dead handler is the point: the builder must reject it
            result = "handled"
        return result
