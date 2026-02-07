"""Test fixture: Action with keyword arguments."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def greet_person(name: str, greeting: str = "Hello") -> str:
    return f"{greeting}, {name}!"


@workflow
class ActionKwargsWorkflow(Workflow):
    """Action called with explicit keyword arguments."""

    async def run(self) -> str:
        result = await greet_person(name="World", greeting="Hi")
        return result
