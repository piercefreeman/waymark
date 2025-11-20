from rappel import action, workflow
from rappel.workflow import Workflow


@action
async def greet(name: str) -> str:
    return f"hello {name}"


@workflow
class IntegrationWorkflow(Workflow):
    async def run(self):
        result = await greet(name="world")
        return result
