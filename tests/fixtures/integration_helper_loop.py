from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def is_even(value: int) -> bool:
    return value % 2 == 0


@action
async def double_value(value: int) -> int:
    return value * 2


@action
async def identity_value(value: int) -> int:
    return value


@action
async def format_total(total: int) -> str:
    return f"total:{total}"


@workflow
class HelperLoopWorkflow(Workflow):
    async def run_internal(self, value: int) -> int:
        if await is_even(value=value):
            return await double_value(value=value)
        return await identity_value(value=value)

    async def run(self, items: list[int]) -> str:
        total = 0
        for value in items:
            total += await self.run_internal(value)
        return await format_total(total=total)
