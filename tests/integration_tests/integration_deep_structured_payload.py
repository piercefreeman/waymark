"""Manual integration fixture reproducing deep structured transport decode failures."""

from datetime import timedelta
from typing import Any

from pydantic import BaseModel

from waymark import action, workflow
from waymark.workflow import Workflow


class DeepPayload(BaseModel):
    payload: dict[str, Any]


def build_nested_payload(depth: int) -> dict[str, Any]:
    value: Any = {"leaf": "ok"}
    for idx in range(depth):
        if idx % 2 == 0:
            value = {"node": value}
        else:
            value = {"items": [value]}
    return {"root": value}


@action
async def emit_deep_payload(depth: int) -> DeepPayload:
    return DeepPayload(payload=build_nested_payload(depth))


@workflow
class DeepStructuredPayloadWorkflow(Workflow):
    """Real workflow fixture that returns a deeply nested structured action payload."""

    async def run(self, depth: int = 30) -> DeepPayload:
        return await self.run_action(
            emit_deep_payload(depth=depth),
            timeout=timedelta(seconds=1),
        )
