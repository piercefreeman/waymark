from rappel import action, workflow
from rappel.workflow import Workflow


@action
async def provide_value() -> int:
    return 10


@action
async def explode(value: int) -> int:
    raise ValueError(f"boom:{value}")


@action
async def cleanup(label: str) -> str:
    return f"handled:{label}"


@workflow
class ExceptionWorkflow(Workflow):
    async def run(self):
        try:
            number = await provide_value()
            await explode(value=number)
        except ValueError:
            result = await cleanup(label="fallback")
        return result
