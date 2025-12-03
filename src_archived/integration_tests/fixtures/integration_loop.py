import asyncio

from rappel import action, workflow
from rappel.workflow import Workflow


@action
async def load_item(name: str) -> str:
    return name


@action
async def decorate_item(item: str) -> str:
    return f"{item}-decorated"


@action
async def finalize_payload(items: list[str]) -> str:
    return ",".join(items)


@workflow
class LoopWorkflow(Workflow):
    async def run(self) -> str:
        seeds = await asyncio.gather(load_item(name="alpha"), load_item(name="beta"))
        outputs = []
        for seed in seeds:
            local_value = f"{seed}-local"
            finalized = await decorate_item(item=local_value)
            outputs.append(finalized)
        result = await finalize_payload(items=outputs)
        return result
