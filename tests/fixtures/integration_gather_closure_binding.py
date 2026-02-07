"""Integration test: asyncio.gather with list comprehension closure binding.

Issue 1: Late-binding closure in asyncio.gather with list comprehensions
When using asyncio.gather with a list comprehension that captures a loop variable,
each coroutine should receive the value at the time it was created, not the last value.
"""

import asyncio
from uuid import UUID, uuid4

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def process_comment(post_id: str, comment_id: UUID) -> dict:
    """Return the comment_id that was actually passed to this action."""
    return {"post_id": post_id, "comment_id": str(comment_id)}


@workflow
class GatherClosureBindingWorkflow(Workflow):
    """Test that each coroutine in asyncio.gather receives distinct loop variable values."""

    async def run(self, post_id: str, comment_ids: list[UUID]) -> list[dict]:
        """
        Expected: Each action receives a different comment_id.
        Bug: All actions receive the last comment_id in the list.
        """
        results = await asyncio.gather(
            *[
                process_comment(post_id=post_id, comment_id=comment_id)
                for comment_id in comment_ids
            ],
            return_exceptions=True,
        )
        return results
