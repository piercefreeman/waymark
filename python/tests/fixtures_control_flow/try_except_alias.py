"""Test fixture: Except clauses that name an exception class through an alias."""

from json import JSONDecodeError as DecodeError

from waymark import action, workflow
from waymark.workflow import Workflow

VE = ValueError


@action
async def flaky_action() -> str:
    return "ok"


@action
async def handle_value() -> str:
    return "value_handled"


@action
async def handle_decode() -> str:
    return "decode_handled"


@workflow
class TryExceptAliasWorkflow(Workflow):
    """Aliased except types resolve to the class's own name, which the VM matches."""

    async def run(self) -> str:
        try:
            result = await flaky_action()
        except VE:
            result = await handle_value()
        except (DecodeError, KeyError):
            result = await handle_decode()
        return result
