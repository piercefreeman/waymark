from waymark import action, workflow
from waymark.workflow import RetryPolicy, Workflow


class CustomError(Exception):
    """Custom exception defined in the same module as the workflow."""

    pass


@action
async def provide_value() -> int:
    return 10


@action
async def explode_custom(value: int) -> int:
    """Raise a custom exception (not a built-in)."""
    raise CustomError(f"custom boom:{value}")


@action
async def cleanup(label: str) -> str:
    return f"handled:{label}"


@workflow
class ExceptionCustomWorkflow(Workflow):
    """Workflow that catches a custom exception type defined in the same module."""

    async def run(self):
        try:
            number = await provide_value()
            await self.run_action(
                explode_custom(value=number),
                retry=RetryPolicy(attempts=1),
            )
        except CustomError:
            result = await cleanup(label="custom_fallback")
        return result
