"""Fixture: while loops are not supported."""

from rappel import Workflow, action, workflow


@action
async def check_condition() -> bool:
    return True


@action
async def do_work() -> int:
    return 1


@workflow
class WhileLoopWorkflow(Workflow):
    """Workflow that uses a while loop - should fail validation."""

    async def run(self) -> int:
        total = 0
        # This should fail: while loops are not supported
        while await check_condition():
            total += await do_work()
        return total
