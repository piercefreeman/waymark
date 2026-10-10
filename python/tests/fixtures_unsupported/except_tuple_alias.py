"""Fixture: an except clause naming a tuple alias, not a class, is rejected."""

from waymark import Workflow, action, workflow

ERRORS = (ValueError, KeyError)


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class ExceptTupleAliasWorkflow(Workflow):
    """Workflow listing `except ERRORS:` - would compile to a class called ERRORS."""

    async def run(self) -> str:
        try:
            result = await flaky_action()
        except ERRORS:
            result = "handled"
        return result
