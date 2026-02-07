"""Integration fixture covering fn_call positional binding and dot access."""

from dataclasses import dataclass

from waymark import action, workflow
from waymark.workflow import Workflow


@dataclass
class CheckRequest:
    user_id: str


@action
async def check_enabled(request: CheckRequest) -> str:
    return request.user_id


class PredictCommon(Workflow):
    async def run_internal(self, user_id: str) -> str:
        result = await self.run_action(check_enabled(CheckRequest(user_id=user_id)))
        return result


@workflow
class PredictWorkflow(PredictCommon):
    async def run(self, user: dict) -> str:
        return await self.run_internal(user.user_id)
