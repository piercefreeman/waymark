import asyncio

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def load_item(name: str) -> str:
    return name


@action
async def decorate_item(item: str) -> str:
    return f"{item}-decorated"


@action
async def format_local_value(seed: str, index: int) -> str:
    return f"{seed}-local-{index}"


@action
async def finalize_payload(items: list[str]) -> str:
    return ",".join(items)


@workflow
class LoopAccumWorkflow(Workflow):
    async def run(self) -> str:
        seeds = await asyncio.gather(
            load_item(name="alpha"),
            load_item(name="beta"),
            return_exceptions=True,
        )
        outputs: list[str] = []
        for seed in seeds:
            index = len(outputs)
            local_value = await format_local_value(seed=seed, index=index)
            finalized = await decorate_item(item=local_value)
            outputs.append(finalized)
        result = await finalize_payload(items=outputs)
        return result
