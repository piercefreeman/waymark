"""Integration fixture for external function calls inside conditionals."""

from rappel import workflow
from rappel.workflow import Workflow


def log_event(value: int) -> None:
    return None


@workflow
class ExternalFnCallWorkflow(Workflow):
    """Workflow that hits an external function call in a conditional branch."""

    async def run(self, value: int) -> str:
        if value > 0:
            log_event(value)
            return "logged"
        return "skipped"
