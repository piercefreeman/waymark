"""Try/except workflow that mutates state and consumes it after the block."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action(name="risky_action")
async def risky_action(should_fail: bool) -> str:
    if should_fail:
        raise ValueError("boom")
    return "ok"


@action(name="annotate_success")
async def annotate_success(value: str, previous: str) -> str:
    return previous + "|success:" + value


@action(name="annotate_recovery")
async def annotate_recovery(reason: str, previous: str) -> str:
    return previous + "|recovered:" + reason


@action(name="finalize_result")
async def finalize_result(attempted: bool, recovered: bool, message: str) -> dict:
    return {"attempted": attempted, "recovered": recovered, "message": message}


@workflow
class TryStatefulOutputsWorkflow(Workflow):
    """Workflow that exercises try/except variable capture/return semantics."""

    async def run(self, should_fail: bool) -> dict:
        recovered = False
        message = "start"

        try:
            value = await risky_action(should_fail)
            message = await annotate_success(value, previous=message)
        except ValueError:
            message = await annotate_recovery("fail", previous=message)
            recovered = True

        return await finalize_result(True, recovered, message)
