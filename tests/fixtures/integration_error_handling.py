from pydantic import BaseModel

from waymark import action, workflow
from waymark.workflow import RetryPolicy, Workflow


class IntentionalError(Exception):
    """Error raised intentionally for demonstration."""

    pass


class ErrorResult(BaseModel):
    attempted: bool
    recovered: bool
    message: str


@action
async def risky_action(should_fail: bool) -> str:
    if should_fail:
        raise IntentionalError("This action failed as requested!")
    return "Action completed successfully"


@action
async def recovery_action(error_message: str) -> str:
    return f"Recovered from error: {error_message}"


@action
async def success_action(result: str) -> str:
    return f"Success path: {result}"


@action
async def build_error_result(attempted: bool, recovered: bool, message: str) -> ErrorResult:
    return ErrorResult(attempted=attempted, recovered=recovered, message=message)


@workflow
class ErrorHandlingWorkflow(Workflow):
    """Workflow mirroring example_app's ErrorHandlingWorkflow with BaseModel output."""

    async def run(self, should_fail: bool) -> ErrorResult:
        recovered = False
        message = ""

        try:
            result = await self.run_action(
                risky_action(should_fail), retry=RetryPolicy(attempts=1)
            )
            message = await success_action(result)
        except IntentionalError:
            message = await recovery_action("IntentionalError was caught")
            recovered = True

        return await build_error_result(True, recovered, message)
