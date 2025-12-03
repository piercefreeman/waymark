from pydantic import BaseModel

from rappel import action, workflow
from rappel.workflow import Workflow, RetryPolicy


class CustomError(Exception):
    """Custom exception defined in the same module as the workflow."""

    pass


class WorkflowResult(BaseModel):
    """Result type that mirrors example app's ErrorResult."""

    attempted: bool
    recovered: bool
    message: str


@action
async def risky_action(should_fail: bool) -> str:
    """An action that may fail based on input."""
    if should_fail:
        raise CustomError("This action failed as requested!")
    return "Action completed successfully"


@action
async def recovery_action(error_message: str) -> str:
    """Recovery action called when risky_action fails."""
    return f"Recovered from error: {error_message}"


@action
async def success_action(result: str) -> str:
    """Called when risky_action succeeds."""
    return f"Success path: {result}"


@workflow
class ExceptionWithSuccessWorkflow(Workflow):
    """
    Workflow that mirrors example app's ErrorHandlingWorkflow structure.

    Key difference from simple test: has both success and failure branches.
    """

    async def run(self, should_fail: bool) -> WorkflowResult:
        recovered = False
        message = ""

        try:
            result = await self.run_action(
                risky_action(should_fail),
                retry=RetryPolicy(attempts=1),
            )
            message = await success_action(result)
        except CustomError:
            recovered_msg = await recovery_action("CustomError was caught")
            recovered = True
            message = recovered_msg

        return WorkflowResult(
            attempted=True,
            recovered=recovered,
            message=message,
        )
