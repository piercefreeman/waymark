"""Test fixture: Unsupported asyncio.gather pattern with variable spread."""

import asyncio

from rappel import workflow
from rappel.workflow import Workflow


@workflow
class GatherUnsupportedVariableWorkflow(Workflow):
    """Unsupported pattern: building tasks list then spreading.

    This pattern cannot be compiled to IR because it requires
    data flow analysis to understand what's in the tasks list.
    """

    async def run(self, count: int) -> list[int]:
        # Build up tasks list in a loop - NOT SUPPORTED
        tasks = []
        for i in range(count):
            tasks.append(i)

        # Spread the variable - this should raise an error
        results = await asyncio.gather(*tasks)
        return results
