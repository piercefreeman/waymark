"""Test fixture: asyncio.gather with tuple-unpacked starred list comprehension."""

import asyncio

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def process_pair(name: str, score: int) -> str:
    return f"{name}:{score}"


@workflow
class GatherListCompTupleUnpackWorkflow(Workflow):
    """Gather spread that destructures each collection item before the action call."""

    async def run(self, pairs: list[tuple[str, int]]) -> list[str | BaseException]:
        results = await asyncio.gather(
            *[process_pair(name=name, score=score) for name, score in pairs],
            return_exceptions=True,
        )
        return results
