"""Repro fixture for action request constructor evaluation."""

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
    async def run_internal(self, user_id: str) -> None:
        await self.run_action(check_enabled(CheckRequest(user_id=user_id)))


@workflow
class ReproActionRequestNullWorkflow(PredictCommon):
    async def run(self, user_id: str) -> None:
        await self.run_internal(user_id)
