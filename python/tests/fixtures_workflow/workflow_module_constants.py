"""Test fixture: module-level constants read in workflow code.

A global the workflow never binds compiles as the literal it holds, so these
reads become values in the IR rather than lookups of variables the runtime
never defined. Names the workflow binds itself, such as `limit` below, keep
Python's scoping and stay ordinary variables.
"""

from enum import Enum

from waymark import action, workflow
from waymark.workflow import Workflow


class Tier(Enum):
    FREE = "free"
    PAID = "paid"


BATCH_SIZE = 5 * 6
PREFIX = "batch" + "-"
DEFAULT_TIER = Tier.PAID
ENABLED = True
TIER_LIMITS = (("free", 10), ("paid", BATCH_SIZE))
STAGES = ("fetch", "score")
limit = 999


@action
async def process(label: str, size: int) -> str:
    """Action receiving constant-derived arguments."""
    return f"{label}:{size}"


@workflow
class WorkflowModuleConstantsWorkflow(Workflow):
    """Workflow reading module constants in expressions, conditions and loops."""

    async def run(self, count: int) -> list[str]:
        results = []
        limit = 0
        for tier, tier_limit in TIER_LIMITS:
            if tier == DEFAULT_TIER:
                limit = tier_limit
        if ENABLED:
            for stage in STAGES:
                label = PREFIX + stage
                result = await process(label=label, size=limit + count)
                results.append(result)
        total = await process(label=PREFIX + "total", size=BATCH_SIZE + count)
        results.append(total)
        return results
