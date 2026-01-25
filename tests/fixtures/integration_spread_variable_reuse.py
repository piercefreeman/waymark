"""Integration test: spread where loop variable name was used in earlier loop.

This tests the exact production pattern where:
1. A variable name (comment_id) is used in an earlier for loop to build a list
2. The SAME variable name is reused in the spread comprehension
3. This could cause variable shadowing issues in rappel's IR

Expected: Each action receives its own distinct comment_id.
Bug: All actions receive the same (last) comment_id from the earlier loop.
"""

import asyncio
from datetime import timedelta
from uuid import UUID

from pydantic import BaseModel

from rappel import action, workflow
from rappel.workflow import RetryPolicy, Workflow


class SpawnEngageRequest(BaseModel):
    post_id: UUID
    comment_id: UUID
    user_id: UUID


class FetchCommentsResponse(BaseModel):
    unhandled_comment_ids: list[UUID]
    is_done: bool


class PollSelfPostRequest(BaseModel):
    post_id: UUID
    user_id: UUID


@action
async def spawn_engage_rappel(request: SpawnEngageRequest) -> dict:
    """Echo back what was received."""
    return {
        "post_id": str(request.post_id),
        "comment_id": str(request.comment_id),
        "user_id": str(request.user_id),
    }


@action
async def fetch_new_comments_rappel(post_id: UUID, iteration: int) -> FetchCommentsResponse:
    """Simulate fetching comments - returns different IDs each iteration."""
    if iteration == 0:
        return FetchCommentsResponse(
            unhandled_comment_ids=[
                UUID("11111111-1111-1111-1111-111111111111"),
            ],
            is_done=False,
        )
    elif iteration == 1:
        return FetchCommentsResponse(
            unhandled_comment_ids=[
                UUID("22222222-2222-2222-2222-222222222222"),
            ],
            is_done=False,
        )
    else:
        return FetchCommentsResponse(
            unhandled_comment_ids=[
                UUID("33333333-3333-3333-3333-333333333333"),
            ],
            is_done=True,
        )


@workflow
class SpreadVariableReuseWorkflow(Workflow):
    """Workflow that reuses loop variable name in spread."""

    async def run(
        self,
        *,
        request: PollSelfPostRequest,
    ) -> list[dict]:
        return await self.fetch_comments(request=request)

    async def fetch_comments(
        self,
        *,
        request: PollSelfPostRequest,
    ) -> list[dict]:
        # Build up new_comment_ids in a loop - using comment_id as the loop variable
        new_comment_ids: list[UUID] = []
        iteration = 0

        while True:
            new_comments = await fetch_new_comments_rappel(
                post_id=request.post_id,
                iteration=iteration,
            )

            # THIS IS THE KEY: using comment_id as loop variable here
            for comment_id in new_comments.unhandled_comment_ids:
                if comment_id not in new_comment_ids:
                    new_comment_ids.append(comment_id)

            if new_comments.is_done:
                break

            iteration = iteration + 1

        # NOW: using the SAME variable name comment_id in the spread
        # The earlier for loop may have left comment_id bound to the last value
        spawn_payloads = await asyncio.gather(
            *[
                self.run_action(
                    spawn_engage_rappel(
                        SpawnEngageRequest(
                            post_id=request.post_id,
                            comment_id=comment_id,
                            user_id=request.user_id,
                        )
                    ),
                    retry=RetryPolicy(attempts=3),
                    timeout=timedelta(seconds=10),
                )
                for comment_id in new_comment_ids
            ],
            return_exceptions=True,
        )
        return spawn_payloads
