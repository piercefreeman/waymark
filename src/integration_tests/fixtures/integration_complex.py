import asyncio
from carabiner_worker import action, workflow
from carabiner_worker.workflow import Workflow


@action
async def fetch_left() -> int:
    return 1


@action
async def fetch_right() -> int:
    return 3


@action
async def double(value: int) -> int:
    return value * 2


@action
async def append_prefix(prefix: str, numbers: list[int]) -> str:
    return f"{prefix}:{','.join(str(n) for n in numbers)}"


@workflow
class ComplexWorkflow(Workflow):
    async def run(self):
        raw = await asyncio.gather(fetch_left(), fetch_right())
        values = [await double(num) for num in raw]
        if sum(values) > 6:
            prefix = "big"
        else:
            prefix = "small"
        computed = [n + 1 for n in values]
        result = await append_prefix(prefix=prefix, numbers=computed)
        return result
