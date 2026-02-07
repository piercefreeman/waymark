"""Test fixture: Simple dataclass assignment."""

from dataclasses import dataclass

from waymark import action, workflow
from waymark.workflow import Workflow


@dataclass
class DataResult:
    """Simple dataclass with two fields."""

    value: int
    message: str


@action
async def fetch_value() -> int:
    return 123


@workflow
class DataclassSimpleWorkflow(Workflow):
    """Workflow that creates a dataclass instance."""

    async def run(self) -> DataResult:
        value = await fetch_value()
        result = DataResult(value=value, message="done")
        return result
