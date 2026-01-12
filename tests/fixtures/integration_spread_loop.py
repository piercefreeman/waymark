"""Integration test: spread inside loops (non-empty and empty)."""

import asyncio
import json

from rappel import action, workflow
from rappel.workflow import Workflow


@action
async def identity(value: int) -> int:
    return value


@action
async def sum_values(values: list[int]) -> int:
    return sum(values)


@action
async def format_results(totals: list[int], empties: list[int]) -> str:
    return json.dumps({"totals": totals, "empties": empties})


@workflow
class SpreadLoopWorkflow(Workflow):
    async def run(self, items: list[int]) -> str:
        totals = []
        for item in items:
            results = await asyncio.gather(
                *[identity(value=value) for value in [item, item + 1]]
            )
            total = await sum_values(values=results)
            totals = totals + [total]

        empties = []
        for _item in items:
            results = await asyncio.gather(*[identity(value=value) for value in []])
            empties = empties + [0]

        return await format_results(totals=totals, empties=empties)
