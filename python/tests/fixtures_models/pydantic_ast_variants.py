"""Test fixtures: Pydantic model constructors in diverse AST contexts."""

from pydantic import BaseModel

from waymark import workflow
from waymark.workflow import Workflow


class SimpleResult(BaseModel):
    """Simple Pydantic model with two fields."""

    value: int
    message: str


@workflow
class PydanticReturnListWorkflow(Workflow):
    """Workflow returning list literal with Pydantic constructors."""

    async def run(self) -> list[SimpleResult]:
        return [SimpleResult(value=1, message="ok")]


@workflow
class PydanticReturnDictWorkflow(Workflow):
    """Workflow returning dict literal with Pydantic constructors."""

    async def run(self) -> dict[str, SimpleResult]:
        return {"item": SimpleResult(value=2, message="ok")}


@workflow
class PydanticAssignmentListWorkflow(Workflow):
    """Workflow assigning list literal with Pydantic constructors."""

    async def run(self) -> list[SimpleResult]:
        items = [SimpleResult(value=3, message="a"), SimpleResult(value=4, message="b")]
        return items


@workflow
class PydanticAssignmentDictWorkflow(Workflow):
    """Workflow assigning dict literal with Pydantic constructors."""

    async def run(self) -> dict[str, SimpleResult]:
        payload = {
            "first": SimpleResult(value=5, message="a"),
            "second": SimpleResult(value=6, message="b"),
        }
        return payload


@workflow
class PydanticAssignmentTupleWorkflow(Workflow):
    """Workflow assigning tuple literal with Pydantic constructors."""

    async def run(self) -> tuple[SimpleResult, SimpleResult]:
        pair = (SimpleResult(value=7, message="a"), SimpleResult(value=8, message="b"))
        return pair
