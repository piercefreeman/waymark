"""Test fixture: Dataclass with positional arguments."""

from dataclasses import dataclass

from rappel import action, workflow
from rappel.workflow import Workflow


@dataclass
class Point:
    """Simple dataclass representing a point."""

    x: int
    y: int


@action
async def get_x() -> int:
    return 5


@action
async def get_y() -> int:
    return 10


@workflow
class DataclassPositionalWorkflow(Workflow):
    """Workflow that creates dataclass with positional args."""

    async def run(self) -> Point:
        x = await get_x()
        y = await get_y()
        # Use positional arguments
        point = Point(x, y)
        return point
