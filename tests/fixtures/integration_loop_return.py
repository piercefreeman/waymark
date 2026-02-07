from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def matches_needle(value: int, needle: int) -> bool:
    return value == needle


@action
async def format_found(value: int, checked: int) -> str:
    return f"found:{value} checked:{checked}"


@action
async def format_not_found(checked: int) -> str:
    return f"not_found checked:{checked}"


@workflow
class LoopReturnWorkflow(Workflow):
    async def run(self, items: list[int], needle: int) -> str:
        checked = 0
        for value in items:
            checked += 1
            is_match = await matches_needle(value=value, needle=needle)
            if is_match:
                result = await format_found(value=value, checked=checked)
                return result

        return await format_not_found(checked=checked)

