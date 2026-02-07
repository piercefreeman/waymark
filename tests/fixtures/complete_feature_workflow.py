"""
Waymark Workflow demonstrating all language features.

This workflow showcases every supported pattern in the Waymark IR:
1. Simple assignments (literals, expressions)
2. Action calls with keyword arguments
3. Parallel execution (asyncio.gather)
4. Spread actions (parallel iteration over collections)
5. For loops (sequential iteration)
6. Conditionals (if/elif/else)
7. Try/except (exception handling)
8. Tuple unpacking from parallel calls
9. List operations (literals, concatenation)
10. Return statements
"""

import asyncio
from typing import Any

from waymark import action, workflow
from waymark.workflow import Workflow


# =============================================================================
# Exception for demonstrating try/except
# =============================================================================


class NetworkError(Exception):
    """Simulated network error for exception handling demo."""
    pass


# =============================================================================
# Actions for demonstrating various patterns
# =============================================================================


@action
async def check_status_a(service: str) -> dict[str, Any]:
    """Check status of service A - used in parallel execution."""
    return {"service": service, "status": "healthy", "latency_ms": 42}


@action
async def check_status_b(service: str) -> dict[str, Any]:
    """Check status of service B - used in parallel execution."""
    return {"service": service, "status": "healthy", "latency_ms": 38}


@action
async def process_item(item: Any) -> dict[str, Any]:
    """Process a single item - used in spread actions."""
    return {"item": item, "processed": True, "result": item * 2 if isinstance(item, int) else str(item)}


@action
async def validate_item(item: Any, index: int) -> dict[str, Any]:
    """Validate an item at a given index - used in for loops."""
    return {"index": index, "item": item, "valid": True}


@action
async def handle_overflow(count: int) -> str:
    """Handle case when count exceeds threshold."""
    return f"overflow_handled:{count}"


@action
async def handle_threshold(count: int) -> str:
    """Handle case when count equals threshold."""
    return f"at_threshold:{count}"


@action
async def handle_normal(count: int) -> str:
    """Handle normal case when count is below threshold."""
    return f"normal:{count}"


@action
async def risky_operation(data: list) -> dict[str, Any]:
    """An operation that might fail - used in try/except."""
    # raise NetworkError("This operation failed as requested!")
    return {"success": True, "data_length": len(data)}


@action
async def fallback_operation(data: list) -> dict[str, Any]:
    """Fallback when risky_operation fails."""
    return {"fallback": True, "data_length": len(data)}


@action
async def aggregate_results(items: list, status_a: dict, status_b: dict, final_status: str, risky_result: dict) -> dict[str, Any]:
    """Aggregate all results into a final summary."""
    return {
        "total_items": len(items),
        "status_a": status_a,
        "status_b": status_b,
        "final_status": final_status,
        "complete": True,
        "risky_result": risky_result,
    }


# =============================================================================
# Complete Feature Demonstration Workflow
# =============================================================================


@workflow
class CompleteFeatureWorkflow(Workflow):
    """
    A workflow demonstrating all Waymark language features.

    This workflow:
    1. Initializes variables with simple assignments
    2. Executes parallel status checks (asyncio.gather)
    3. Uses spread to process items in parallel
    4. Iterates sequentially with a for loop
    5. Makes conditional decisions (if/elif/else)
    6. Handles exceptions with try/except
    7. Aggregates everything into a final result
    """

    async def run(self, items: list, threshold: int) -> dict[str, Any]:
        # 1. Simple assignments
        count = 0
        results = []

        # 2. Parallel execution (asyncio.gather with multiple calls)
        status_a, status_b = await asyncio.gather(
            check_status_a(service="alpha"),
            check_status_b(service="beta"),
            return_exceptions=True,
        )

        # 3. Spread action (parallel iteration over a collection)
        processed = await asyncio.gather(
            *[process_item(item=x) for x in items],
            return_exceptions=True,
        )

        # 4. For loop with single action per iteration
        for i, item in enumerate(processed):
            item_result = await validate_item(item=item, index=i)
            results = results + [item_result]

        # 5. Conditional (if/elif/else)
        if count > threshold:
            final_status = await handle_overflow(count=count)
        elif count == threshold:
            final_status = await handle_threshold(count=count)
        else:
            final_status = await handle_normal(count=count)

        # 6. Try/except with exception handling
        try:
            risky_result = await risky_operation(data=results)
        except NetworkError:
            risky_result = await fallback_operation(data=results)

        # 7. Final aggregation and return
        summary = await aggregate_results(
            items=processed,
            status_a=status_a,
            status_b=status_b,
            final_status=final_status,
            risky_result=risky_result,
        )

        return summary
