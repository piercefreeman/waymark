"""Immediate conditional workflow with required input."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def evaluate_high(value: int) -> str:
    return f"high:{value}"


@action
async def evaluate_low(value: int) -> str:
    return f"low:{value}"


@workflow
class ImmediateRequiredInputWorkflow(Workflow):
    async def run(self, value: int) -> str:
        if value >= 10:
            result = await evaluate_high(value=value)
        else:
            result = await evaluate_low(value=value)

        return result
