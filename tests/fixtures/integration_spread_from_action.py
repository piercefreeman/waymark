"""Integration test workflow: spread after an action returns items."""

import asyncio

from rappel import action, workflow
from rappel.workflow import Workflow


@action
async def fetch_items(include_items: bool) -> list[str]:
    if include_items:
        return ["a", "b"]
    return []


@action
async def process_item(item: str) -> str:
    return f"processed:{item}"


@action
async def combine_results(results: list[str]) -> str:
    if not results:
        return "empty"
    return ",".join(results)


@workflow
class SpreadFromActionWorkflow(Workflow):
    async def run(self, include_items: bool) -> str:
        items = await fetch_items(include_items=include_items)
        results = await asyncio.gather(*[process_item(item=item) for item in items])
        return await combine_results(results=results)
