"""
Minimal reproduction case for exception handling inside a for loop.

This tests the scenario where:
1. A for loop iterates over items
2. An action inside the loop raises an exception
3. The exception is caught by a try-except block
4. The loop should continue to the next iteration

Bug: After the exception handler runs, the loop-back edge is not followed,
causing the workflow to stall.
"""

from rappel import action, workflow
from rappel.workflow import Workflow, RetryPolicy


class ItemProcessingError(Exception):
    """Exception raised when item processing fails."""
    pass


@action
async def process_item(item: str) -> str:
    """Process an item - fails for specific items."""
    if item == "bad":
        raise ItemProcessingError(f"Failed to process: {item}")
    return f"processed:{item}"


@action
async def finalize_results(results: list[str], error_count: int) -> dict:
    """Finalize the results with error count."""
    return {"results": results, "errors": error_count}


@workflow
class LoopExceptionWorkflow(Workflow):
    """
    Workflow that processes items in a loop, catching exceptions.

    Expected behavior:
    - Process ["good1", "bad", "good2"]
    - "good1" succeeds -> results = ["processed:good1"]
    - "bad" fails -> exception caught, error_count = 1
    - "good2" succeeds -> results = ["processed:good1", "processed:good2"]
    - Returns {"results": ["processed:good1", "processed:good2"], "errors": 1}

    Bug behavior:
    - After "bad" fails and exception is caught, loop stalls
    - "good2" is never processed
    """

    async def run(self, items: list[str]) -> dict:
        results = []
        error_count = 0

        for item in items:
            try:
                result = await self.run_action(
                    process_item(item=item),
                    retry=RetryPolicy(attempts=1),  # No retries, fail immediately
                )
                results.append(result)
            except ItemProcessingError:
                # This inline assignment should complete, then loop should continue
                error_count = error_count + 1

        final = await finalize_results(results=results, error_count=error_count)
        return final
