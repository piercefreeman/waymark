"""Integration test: loop break/continue with enumerate."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def format_loop(values: list[int], indices: list[int]) -> str:
    values_text = ",".join(str(value) for value in values)
    indices_text = ",".join(str(index) for index in indices)
    return f"values:{values_text}|indices:{indices_text}"


@workflow
class LoopControlFlowWorkflow(Workflow):
    async def run(self) -> str:
        values: list[int] = []
        indices: list[int] = []
        for idx, value in enumerate([1, 2, 3, 4]):
            if value == 2:
                continue
            values.append(value)
            indices.append(idx)
            if value == 3:
                break
        return await format_loop(values=values, indices=indices)
