"""Test fixture: from asyncio import sleep as alias pattern."""

from asyncio import sleep as async_sleep

from rappel import action, workflow
from rappel.workflow import Workflow


@action
async def get_value_aliased_import() -> int:
    return 42


@action
async def format_done_aliased_import(value: int) -> str:
    return f"done:{value}"


@workflow
class SleepAliasedImportWorkflow(Workflow):
    async def run(self) -> str:
        val = await get_value_aliased_import()
        await async_sleep(3)
        return await format_done_aliased_import(value=val)
