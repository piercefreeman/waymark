"""Simple workflow fixture for integration testing."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def greet(name: str) -> str:
    """Simple action that returns a greeting."""
    return f"hello {name}"


@workflow
class SimpleWorkflow(Workflow):
    """A simple workflow with one action call."""

    async def run(self, name: str = "world") -> str:
        result = await greet(name=name)
        return result
