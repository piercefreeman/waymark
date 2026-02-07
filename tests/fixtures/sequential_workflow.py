"""Sequential workflow fixture - tests executing multiple actions in sequence."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def fetch_value() -> int:
    """Fetch an initial value."""
    return 42


@action
async def transform_value(value: int) -> int:
    """Transform the value by doubling it."""
    return value * 2


@action
async def format_result(value: int) -> str:
    """Format the final result."""
    return "result:" + str(value)


@workflow
class SequentialWorkflow(Workflow):
    """Workflow that executes three actions in sequence."""

    async def run(self) -> str:
        initial = await fetch_value()
        doubled = await transform_value(value=initial)
        result = await format_result(value=doubled)
        return result
