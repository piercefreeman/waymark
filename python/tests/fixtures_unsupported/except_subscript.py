"""Fixture: an except clause whose type is not a class name is not supported."""

from waymark import Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class ExceptSubscriptWorkflow(Workflow):
    """Workflow listing an except type by subscript - should fail validation."""

    async def run(self, errors: list) -> str:
        try:
            result = await flaky_action()
        except errors[0]:
            result = "handled"
        return result
