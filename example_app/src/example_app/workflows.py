"""
Workflow definitions showcasing different rappel patterns.

This module contains example workflows demonstrating:
1. Parallel execution with asyncio.gather
2. Sequential chaining
3. Conditional branching (if/else)
4. Loop iteration
5. Error handling with try/except
6. Durable sleep
"""

import asyncio
from datetime import timedelta
from typing import Literal

from pydantic import BaseModel, Field

from rappel import Workflow, action, workflow
from rappel.workflow import RetryPolicy


# =============================================================================
# Shared Models
# =============================================================================


class ComputationResult(BaseModel):
    """Result from the parallel math workflow."""

    input_number: int
    factorial: int
    fibonacci: int
    summary: str


class ComputationRequest(BaseModel):
    number: int = Field(ge=1, le=10, description="Number to feed into the workflow")


class ChainResult(BaseModel):
    """Result from the sequential chain workflow."""

    original: str
    steps: list[str]
    final: str


class ChainRequest(BaseModel):
    text: str = Field(min_length=1, max_length=100, description="Text to transform")


class BranchResult(BaseModel):
    """Result from the conditional branching workflow."""

    value: int
    branch_taken: Literal["high", "medium", "low"]
    message: str


class BranchRequest(BaseModel):
    value: int = Field(description="Value to evaluate (determines which branch)")


class LoopResult(BaseModel):
    """Result from the loop workflow."""

    items: list[str]
    processed: list[str]
    count: int


class LoopRequest(BaseModel):
    items: list[str] = Field(
        min_length=1, max_length=5, description="Items to process in a loop"
    )


class ErrorResult(BaseModel):
    """Result from the error handling workflow."""

    attempted: bool
    recovered: bool
    message: str


class ErrorRequest(BaseModel):
    should_fail: bool = Field(description="Whether the action should fail")


class SleepResult(BaseModel):
    """Result from the durable sleep workflow."""

    started_at: str
    resumed_at: str
    sleep_seconds: int
    message: str


class SleepRequest(BaseModel):
    seconds: int = Field(ge=1, le=10, description="Seconds to sleep (1-10)")


# =============================================================================
# Actions - Parallel Workflow
# =============================================================================


@action
async def compute_factorial(n: int) -> int:
    """Compute factorial of n."""
    total = 1
    for value in range(2, n + 1):
        total *= value
        await asyncio.sleep(0)
    return total


@action
async def compute_fibonacci(n: int) -> int:
    """Compute the nth Fibonacci number."""
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
    """Summarize the parallel computation results."""
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


# =============================================================================
# Actions - Sequential Chain Workflow
# =============================================================================


@action
async def step_uppercase(text: str) -> str:
    """Convert text to uppercase."""
    await asyncio.sleep(0.1)
    return text.upper()


@action
async def step_reverse(text: str) -> str:
    """Reverse the text."""
    await asyncio.sleep(0.1)
    return text[::-1]


@action
async def step_add_stars(text: str) -> str:
    """Add stars around the text."""
    await asyncio.sleep(0.1)
    return "*** " + text + " ***"


@action
async def build_chain_result(
    original: str, step1: str, step2: str, step3: str
) -> ChainResult:
    """Build the chain result with formatted steps."""
    return ChainResult(
        original=original,
        steps=[
            "uppercase: " + step1,
            "reverse: " + step2,
            "stars: " + step3,
        ],
        final=step3,
    )


# =============================================================================
# Actions - Conditional Branch Workflow
# =============================================================================


@action
async def evaluate_high(value: int) -> BranchResult:
    """Handle high values (>= 75)."""
    await asyncio.sleep(0.1)
    return BranchResult(
        value=value,
        branch_taken="high",
        message="High value detected: " + str(value) + " is in the top tier!",
    )


@action
async def evaluate_medium(value: int) -> BranchResult:
    """Handle medium values (25-74)."""
    await asyncio.sleep(0.1)
    return BranchResult(
        value=value,
        branch_taken="medium",
        message="Medium value: " + str(value) + " is in the middle range.",
    )


@action
async def evaluate_low(value: int) -> BranchResult:
    """Handle low values (< 25)."""
    await asyncio.sleep(0.1)
    return BranchResult(
        value=value,
        branch_taken="low",
        message="Low value: " + str(value) + " is in the bottom tier.",
    )


# =============================================================================
# Actions - Loop Workflow
# =============================================================================


@action
async def process_item(item: str) -> str:
    """Process a single item in the loop."""
    await asyncio.sleep(0.1)
    return item.upper()


@action
async def build_loop_result(items: list[str], processed: list[str]) -> LoopResult:
    """Build the final loop result."""
    await asyncio.sleep(0.1)
    return LoopResult(items=items, processed=processed, count=len(processed))


# =============================================================================
# Actions - Error Handling Workflow
# =============================================================================


class IntentionalError(Exception):
    """Error raised intentionally for demonstration."""

    pass


@action
async def risky_action(should_fail: bool) -> str:
    """An action that may fail based on input."""
    await asyncio.sleep(0.1)
    if should_fail:
        raise IntentionalError("This action failed as requested!")
    return "Action completed successfully"


@action
async def recovery_action(error_message: str) -> str:
    """Recovery action called when risky_action fails."""
    await asyncio.sleep(0.1)
    return f"Recovered from error: {error_message}"


@action
async def success_action(result: str) -> str:
    """Called when risky_action succeeds."""
    await asyncio.sleep(0.1)
    return f"Success path: {result}"


@action
async def build_error_result(
    attempted: bool, recovered: bool, message: str
) -> ErrorResult:
    """Build the error handling result."""
    return ErrorResult(attempted=attempted, recovered=recovered, message=message)


# =============================================================================
# Actions - Sleep Workflow
# =============================================================================


@action
async def get_timestamp() -> str:
    """Get current timestamp as string."""
    from datetime import datetime

    return datetime.now().isoformat()


@action
async def format_sleep_result(
    started: str, resumed: str, seconds: int
) -> SleepResult:
    """Format the sleep workflow result."""
    return SleepResult(
        started_at=started,
        resumed_at=resumed,
        sleep_seconds=seconds,
        message=f"Slept for {seconds} seconds between {started} and {resumed}",
    )


# =============================================================================
# Workflow Definitions
# =============================================================================


@workflow
class ParallelMathWorkflow(Workflow):
    """
    Demonstrates parallel execution using asyncio.gather.

    Two independent computations (factorial and fibonacci) run in parallel,
    then their results are combined in a final action.
    """

    async def run(self, number: int) -> ComputationResult:
        # Fan out: compute factorial and fibonacci in parallel
        factorial_value, fib_value = await asyncio.gather(
            compute_factorial(number),
            compute_fibonacci(number),
        )
        # Fan in: combine results
        result = await summarize_math(
            input_number=number,
            factorial_value=factorial_value,
            fibonacci_value=fib_value,
        )
        return result


@workflow
class SequentialChainWorkflow(Workflow):
    """
    Demonstrates sequential action chaining.

    Each action depends on the output of the previous one,
    creating a pipeline of transformations.
    """

    async def run(self, text: str) -> ChainResult:
        # Step 1: Uppercase
        step1 = await step_uppercase(text)

        # Step 2: Reverse
        step2 = await step_reverse(step1)

        # Step 3: Add stars
        step3 = await step_add_stars(step2)

        # Build result in action (f-strings not supported in workflow)
        return await build_chain_result(text, step1, step2, step3)


@workflow
class ConditionalBranchWorkflow(Workflow):
    """
    Demonstrates conditional branching with if/else.

    Different actions are executed based on the input value,
    showing how rappel handles control flow.
    """

    async def run(self, value: int) -> BranchResult:
        if value >= 75:
            return await evaluate_high(value)
        elif value >= 25:
            return await evaluate_medium(value)
        else:
            return await evaluate_low(value)


@workflow
class LoopProcessingWorkflow(Workflow):
    """
    Demonstrates loop iteration over a collection.

    Each item in the input list is processed by an action,
    and results are accumulated. Uses the loop controller pattern.
    """

    async def run(self, items: list[str]) -> LoopResult:
        # Use the loop controller pattern: accumulator, loop with action, append
        processed = []
        for item in items:
            result = await process_item(item)
            processed.append(result)

        # Build the result in an action (constructors aren't supported in return)
        return await build_loop_result(items, processed)


@workflow
class ErrorHandlingWorkflow(Workflow):
    """
    Demonstrates error handling with try/except.

    Shows how rappel handles exceptions and allows
    recovery through exception handlers.
    """

    async def run(self, should_fail: bool) -> ErrorResult:
        # Use a variable to collect result instead of multiple returns
        recovered = False
        message = ""

        try:
            result = await self.run_action(
                risky_action(should_fail),
                retry=RetryPolicy(attempts=1),  # Don't retry, let it fail
            )
            message = await success_action(result)
        except IntentionalError:
            recovered_msg = await recovery_action("IntentionalError was caught")
            recovered = True
            message = recovered_msg

        # Build result in action (constructors aren't supported in return)
        return await build_error_result(True, recovered, message)


@workflow
class DurableSleepWorkflow(Workflow):
    """
    Demonstrates durable sleep with asyncio.sleep.

    The workflow pauses for a specified duration. Unlike regular sleep,
    this is durable - if the worker crashes, the sleep continues
    from where it left off after recovery.
    """

    async def run(self, seconds: int) -> SleepResult:
        started = await get_timestamp()

        # Durable sleep - survives worker restarts
        await asyncio.sleep(seconds)

        resumed = await get_timestamp()

        return await format_sleep_result(started, resumed, seconds)


# =============================================================================
# Legacy alias for backwards compatibility
# =============================================================================

ExampleMathWorkflow = ParallelMathWorkflow
