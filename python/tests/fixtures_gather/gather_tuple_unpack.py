"""Test fixture: asyncio.gather with tuple unpacking."""

import asyncio

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def compute_factorial(n: int) -> int:
    """Compute factorial of n."""
    result = 1
    for i in range(1, n + 1):
        result *= i
    return result


@action
async def compute_fibonacci(n: int) -> int:
    """Compute nth Fibonacci number."""
    if n <= 1:
        return n
    a, b = 0, 1
    for _ in range(2, n + 1):
        a, b = b, a + b
    return b


@action
async def summarize_math(
    factorial_value: int | BaseException, fib_value: int | BaseException
) -> str:
    """Summarize the computed values."""
    return f"factorial={factorial_value}, fibonacci={fib_value}"


@workflow
class GatherTupleUnpackWorkflow(Workflow):
    """Gather results unpacked into multiple variables."""

    async def run(self) -> str:
        # This should unpack the parallel results into separate variables
        factorial_value, fib_value = await asyncio.gather(
            compute_factorial(n=5),
            compute_fibonacci(n=10),
            return_exceptions=True,
        )
        # Use the unpacked values
        summary = await summarize_math(
            factorial_value=factorial_value,
            fib_value=fib_value,
        )
        return summary
