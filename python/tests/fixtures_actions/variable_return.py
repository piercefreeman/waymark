"""Fixture: returning a variable is valid."""

from rappel import Workflow, action, workflow


@action
async def compute_result_for_variable_return(x: int) -> int:
    return x * 2


@workflow
class VariableReturnWorkflow(Workflow):
    """Workflow that returns a variable - valid pattern."""

    async def run(self, value: int) -> int:
        result = await compute_result_for_variable_return(value)
        # This is valid: returning a variable
        return result
