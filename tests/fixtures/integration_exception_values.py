from waymark import action, workflow
from waymark.workflow import Workflow


class CustomError(Exception):
    def __init__(self, message: str, code: int) -> None:
        super().__init__(message)
        self.code = code


@action
async def risky_value() -> str:
    raise CustomError("boom", 418)


@action
async def format_exception(exc: dict) -> str:
    return f"{exc['type']}:{exc['values']['code']}"


@workflow
class ExceptionValuesWorkflow(Workflow):
    async def run(self) -> str:
        try:
            await self.run_action(risky_value())
        except CustomError as err:
            return await self.run_action(format_exception(exc=err))
        return "no-error"
