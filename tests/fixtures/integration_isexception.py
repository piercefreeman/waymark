"""Integration test: isexception builtin and exception dot access."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def boom() -> None:
    raise ValueError("boom")


@action
async def format_result(kind: str, message: str, is_value: bool) -> str:
    return f"{kind}:{message}:{is_value}"


@workflow
class IsExceptionWorkflow(Workflow):
    async def run(self) -> str:
        try:
            await boom()
            return await format_result(kind="none", message="none", is_value=False)
        except Exception as err:
            is_value = isinstance(err, ValueError)
            kind = "other"
            if is_value:
                kind = "value"
            message = err.message
            return await format_result(kind=kind, message=message, is_value=is_value)
