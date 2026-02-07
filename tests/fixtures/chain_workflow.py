"""Chain workflow fixture - tests sequential action chaining like example_app.

This matches the SequentialChainWorkflow pattern from example_app:
1. Each action depends on the output of the previous one
2. Final action takes all intermediate results and produces combined output
"""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def step_uppercase(text: str) -> str:
    """Convert text to uppercase."""
    return text.upper()


@action
async def step_reverse(text: str) -> str:
    """Reverse the text."""
    return text[::-1]


@action
async def step_add_stars(text: str) -> str:
    """Add stars around the text."""
    return "*** " + text + " ***"


@action
async def build_chain_result(original: str, step1: str, step2: str, step3: str) -> str:
    """Build the chain result with all steps."""
    return "original:" + original + ",step1:" + step1 + ",step2:" + step2 + ",step3:" + step3


@workflow
class ChainWorkflow(Workflow):
    """Workflow that chains transformations and combines all results."""

    async def run(self, text: str) -> str:
        # Step 1: Uppercase
        step1 = await step_uppercase(text)

        # Step 2: Reverse
        step2 = await step_reverse(step1)

        # Step 3: Add stars
        step3 = await step_add_stars(step2)

        # Build result using all intermediate values
        return await build_chain_result(text, step1, step2, step3)
