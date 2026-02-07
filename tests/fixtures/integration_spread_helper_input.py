"""Integration test: spread collection uses a helper input derived from an action."""

import asyncio

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def fetch_items() -> list[str]:
    return ["a", "b"]


@action
async def check_ready() -> bool:
    return True


@action
async def process_item(item: str) -> str:
    return f"processed:{item}"


@action
async def combine_results(results: list[str]) -> str:
    return ",".join(results)


@workflow
class SpreadHelperInputWorkflow(Workflow):
    async def _process(self, items: list[str]) -> list[str]:
        ready = await check_ready()
        if ready:
            results = await asyncio.gather(
                *[process_item(item=item) for item in items],
                return_exceptions=True,
            )
            return results
        return []

    async def run(self) -> str:
        items = await fetch_items()
        results = await self._process(items)
        return await combine_results(results=results)
