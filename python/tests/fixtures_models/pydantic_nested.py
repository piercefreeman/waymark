"""Test fixture: Pydantic model with nested model."""

from pydantic import BaseModel

from rappel import action, workflow
from rappel.workflow import Workflow


class InnerModel(BaseModel):
    """Inner nested model."""

    x: int
    y: int


class OuterModel(BaseModel):
    """Outer model containing inner model."""

    name: str
    inner: dict  # Stored as dict since we can't serialize nested models


@action
async def get_coordinates() -> dict:
    return {"x": 10, "y": 20}


@workflow
class PydanticNestedWorkflow(Workflow):
    """Workflow with nested model construction."""

    async def run(self) -> dict:
        coords = await get_coordinates()
        # Inner is passed as dict
        result = OuterModel(name="test", inner=coords)
        return result
