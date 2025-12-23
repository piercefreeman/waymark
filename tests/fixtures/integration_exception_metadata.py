from rappel import action, workflow
from rappel.workflow import Workflow


class MetadataError(Exception):
    def __init__(self, message: str, code: int, detail: str) -> None:
        super().__init__(message)
        self.code = code
        self.detail = detail


@action
async def risky_metadata() -> str:
    raise MetadataError("Metadata error triggered", 418, "teapot")


@action
async def format_captured(exc_type: str, code: int, detail: str) -> str:
    return f"{exc_type}:{code}:{detail}"


@workflow
class ExceptionMetadataWorkflow(Workflow):
    async def run(self) -> str:
        try:
            await self.run_action(risky_metadata())
        except MetadataError as err:
            return await self.run_action(
                format_captured(exc_type=err.type, code=err.code, detail=err.detail)
            )
        return "no-error"
