"""
Workflow definitions showcasing different rappel patterns.

This module contains example workflows demonstrating:
1. Parallel execution with asyncio.gather
2. Sequential chaining
3. Conditional branching (if/else)
4. Loop iteration
5. Return inside a loop
6. Error handling with try/except
7. Durable sleep
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


class LoopReturnResult(BaseModel):
    """Result from the early-return loop workflow."""

    items: list[int]
    needle: int
    found: bool
    value: int | None
    checked: int


class LoopReturnRequest(BaseModel):
    items: list[int] = Field(
        min_length=1, max_length=10, description="Items to search in a loop"
    )
    needle: int = Field(description="Value to search for (returns early when found)")


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


class GuardFallbackResult(BaseModel):
    """Result from the default-else continuation workflow."""

    user: str
    note_count: int
    summary: str


class GuardFallbackRequest(BaseModel):
    user: str = Field(
        min_length=1,
        max_length=30,
        description="User to summarize. Use 'empty' to trigger the empty path.",
    )


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


@action
async def matches_needle(value: int, needle: int) -> bool:
    """Check whether the current loop value matches the needle."""
    await asyncio.sleep(0.05)
    return value == needle


@action
async def build_loop_return_result(
    items: list[int],
    needle: int,
    found: bool,
    value: int | None,
    checked: int,
) -> LoopReturnResult:
    """Build the early-return loop result."""
    await asyncio.sleep(0)
    return LoopReturnResult(
        items=items,
        needle=needle,
        found=found,
        value=value,
        checked=checked,
    )


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
class LoopReturnWorkflow(Workflow):
    """
    Demonstrates returning from inside a for-loop.

    Returns as soon as the needle is found in the input list.
    """

    async def run(self, items: list[int], needle: int) -> LoopReturnResult:
        checked = 0
        for value in items:
            checked += 1
            is_match = await matches_needle(value=value, needle=needle)
            if is_match:
                result = await build_loop_return_result(
                    items=items,
                    needle=needle,
                    found=True,
                    value=value,
                    checked=checked,
                )
                return result

        return await build_loop_return_result(
            items=items,
            needle=needle,
            found=False,
            value=None,
            checked=checked,
        )


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
# Actions - Early Return with Loop Workflow
# =============================================================================


class ParseResult(BaseModel):
    """Result from parsing that may or may not have items to process."""

    session_id: str | None
    items: list[str]
    new_items: list[str]


class ProcessedItemResult(BaseModel):
    """Result from processing individual items."""

    item_id: str
    processed: bool


class EarlyReturnLoopResult(BaseModel):
    """Final result from early return + loop workflow."""

    had_session: bool
    processed_count: int
    all_items: list[str]


@action
async def parse_input_data(input_text: str) -> ParseResult:
    """
    Parse input data and return result with optional session.

    If input starts with 'no_session:', returns None session_id.
    Otherwise returns a session and parses items from the input.
    """
    await asyncio.sleep(0.05)

    if input_text.startswith("no_session:"):
        return ParseResult(
            session_id=None,
            items=[],
            new_items=[],
        )

    # Parse items from input (comma separated)
    items = [s.strip() for s in input_text.split(",") if s.strip()]
    return ParseResult(
        session_id="session-123",
        items=items,
        new_items=items,  # In real workflow, this might be a subset
    )


@action
async def process_single_item(item: str, session_id: str) -> ProcessedItemResult:
    """Process a single item using the session."""
    await asyncio.sleep(0.05)
    return ProcessedItemResult(
        item_id=f"processed-{item}",
        processed=True,
    )


@action
async def finalize_processing(items: list[str], processed_count: int) -> EarlyReturnLoopResult:
    """Finalize the processing results."""
    await asyncio.sleep(0.05)
    return EarlyReturnLoopResult(
        had_session=True,
        processed_count=processed_count,
        all_items=items,
    )


@action
async def build_empty_result() -> EarlyReturnLoopResult:
    """Build an empty result when no session exists."""
    return EarlyReturnLoopResult(
        had_session=False,
        processed_count=0,
        all_items=[],
    )


@workflow
class EarlyReturnLoopWorkflow(Workflow):
    """
    Demonstrates the pattern: if-with-early-return followed by for-loop.

    This pattern is common in data processing workflows where:
    1. First action parses/validates input and returns metadata
    2. If validation fails (e.g., no session), return early
    3. Otherwise, loop over items from the result and process each
    4. Finalize with aggregated results

    This tests the DAG's handling of:
    - If without else clause where the 'then' branch has a return
    - Continuation to for-loop when the if condition is false
    - Loop iteration over result fields
    """

    async def run(self, input_text: str) -> EarlyReturnLoopResult:
        # Step 1: Parse the input
        parse_result = await parse_input_data(input_text)

        # Step 2: Early return if no session
        if not parse_result.session_id:
            return await build_empty_result()

        # Step 3: Loop over new items and process each
        processed_count = 0
        for item in parse_result.new_items:
            await process_single_item(item, parse_result.session_id)
            processed_count = processed_count + 1

        # Step 4: Finalize
        return await finalize_processing(parse_result.items, processed_count)


# =============================================================================
# Actions - Guard Fallback Workflow
# =============================================================================


@action
async def fetch_recent_notes(user: str) -> list[str]:
    """Return recent notes or an empty list for the empty path."""
    await asyncio.sleep(0.05)
    if user.lower() == "empty":
        return []
    return [f"{user}-note-1", f"{user}-note-2"]


@action
async def summarize_notes(notes: list[str]) -> str:
    """Summarize a list of notes."""
    await asyncio.sleep(0.05)
    return " | ".join(notes)


@action
async def build_guard_fallback_result(
    user: str, note_count: int, summary: str
) -> GuardFallbackResult:
    """Build the guard fallback result."""
    await asyncio.sleep(0)
    return GuardFallbackResult(
        user=user,
        note_count=note_count,
        summary=summary,
    )


@workflow
class GuardFallbackWorkflow(Workflow):
    """
    Demonstrates an if without else that still continues.

    If the notes list is empty, we skip the summarize action and fall through.
    """

    async def run(self, user: str) -> GuardFallbackResult:
        notes = await fetch_recent_notes(user)
        summary = "no notes found"
        if notes:
            summary = await summarize_notes(notes)
        return await build_guard_fallback_result(user, len(notes), summary)


# =============================================================================
# Legacy alias for backwards compatibility
# =============================================================================

ExampleMathWorkflow = ParallelMathWorkflow
