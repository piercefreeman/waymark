import asyncio

from pydantic import BaseModel

from waymark import action, workflow
from waymark.workflow import Workflow


class ComputationResult(BaseModel):
    input_number: int
    factorial: int
    fibonacci: int
    summary: str


@action
async def compute_factorial(n: int) -> int:
    total = 1
    for value in range(2, n + 1):
        total *= value
        await asyncio.sleep(0)
    return total


@action
async def compute_fibonacci(n: int) -> int:
    previous, current = 0, 1
    for _ in range(n):
        previous, current = current, previous + current
        await asyncio.sleep(0)
    return previous


@action
async def summarize_math(
    *,
    input_number: int,
    factorial_value: int,
    fibonacci_value: int,
) -> ComputationResult:
    if factorial_value > 5_000:
        summary = f"{input_number}! is massive compared to Fib({input_number})={fibonacci_value}"
    elif factorial_value > 100:
        summary = f"{input_number}! is larger, but Fibonacci is {fibonacci_value}"
    else:
        summary = f"{input_number}! ({factorial_value}) stays tame next to Fibonacci={fibonacci_value}"
    return ComputationResult(
        input_number=input_number,
        factorial=factorial_value,
        fibonacci=fibonacci_value,
        summary=summary,
    )


@workflow
class ParallelMathWorkflow(Workflow):
    async def run(self, number: int) -> ComputationResult:
        factorial_value, fib_value = await asyncio.gather(
            compute_factorial(number),
            compute_fibonacci(number),
            return_exceptions=True,
        )
        return await summarize_math(
            input_number=number,
            factorial_value=factorial_value,
            fibonacci_value=fib_value,
        )
