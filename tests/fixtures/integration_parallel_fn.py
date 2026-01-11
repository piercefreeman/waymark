"""Integration test: asyncio.gather with workflow helper methods (not just actions)."""

import asyncio

from rappel import action, workflow
from rappel.workflow import Workflow


@action
async def multiply(x: int, y: int) -> int:
    """Multiply two numbers."""
    return x * y


@action
async def add(x: int, y: int) -> int:
    """Add two numbers."""
    return x + y


@workflow
class ParallelFnWorkflow(Workflow):
    """Workflow that calls helper methods in parallel via asyncio.gather."""

    async def helper_double(self, n: int) -> int:
        """Helper method that doubles a number using an action."""
        result = await multiply(x=n, y=2)
        return result

    async def helper_triple(self, n: int) -> int:
        """Helper method that triples a number using an action."""
        result = await multiply(x=n, y=3)
        return result

    async def run(self, value: int) -> int:
        # Call two helper methods in parallel
        doubled, tripled = await asyncio.gather(
            self.helper_double(n=value),
            self.helper_triple(n=value),
        )
        # Sum the results
        total = await add(x=doubled, y=tripled)
        return total
