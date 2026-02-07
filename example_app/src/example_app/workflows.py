"""
Workflow definitions showcasing different waymark patterns.

This module contains example workflows demonstrating:
1. Parallel execution with asyncio.gather
2. Sequential chaining
3. Conditional branching (if/else)
4. Loop iteration
5. While loops
6. Return inside a loop
7. Error handling with try/except
8. Durable sleep
"""

import asyncio
from typing import Literal

from pydantic import BaseModel, Field
from waymark import Workflow, action, workflow
from waymark.workflow import RetryPolicy

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


class WhileLoopResult(BaseModel):
    """Result from the while loop workflow."""

    limit: int
    final: int
    iterations: int


class WhileLoopRequest(BaseModel):
    limit: int = Field(ge=1, le=10, description="Upper bound for the while loop")


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
    error_type: str | None = None
    error_code: int | None = None
    error_detail: str | None = None


class ErrorRequest(BaseModel):
    should_fail: bool = Field(description="Whether the action should fail")


class SleepResult(BaseModel):
    """Result from the durable sleep workflow."""

    started_at: str
    resumed_at: str
    sleep_seconds: int
    message: str


class SleepRequest(BaseModel):
    seconds: int = Field(ge=1, le=60, description="Seconds to sleep (1-10)")


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


class KwOnlyLocationRequest(BaseModel):
    latitude: float | None = Field(
        default=None, description="Optional latitude for the target location."
    )
    longitude: float | None = Field(
        default=None, description="Optional longitude for the target location."
    )


class KwOnlyLocationResult(BaseModel):
    latitude: float | None
    longitude: float | None
    message: str


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
# Actions - While Loop Workflow
# =============================================================================


@action
async def increment_counter(value: int) -> int:
    """Increment the loop counter."""
    await asyncio.sleep(0.05)
    return value + 1


@action
async def build_while_result(
    limit: int,
    final: int,
    iterations: int,
) -> WhileLoopResult:
    """Build the final while loop result."""
    await asyncio.sleep(0.05)
    return WhileLoopResult(limit=limit, final=final, iterations=iterations)


# =============================================================================
# Actions - Error Handling Workflow
# =============================================================================


class IntentionalError(Exception):
    """Error raised intentionally for demonstration."""

    pass


class ExceptionMetadataError(Exception):
    """Error with attached metadata for exception value capture."""

    def __init__(self, message: str, code: int, detail: str) -> None:
        super().__init__(message)
        self.code = code
        self.detail = detail


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
    attempted: bool,
    recovered: bool,
    message: str,
    error_type: str | None = None,
    error_code: int | None = None,
    error_detail: str | None = None,
) -> ErrorResult:
    """Build the error handling result."""
    return ErrorResult(
        attempted=attempted,
        recovered=recovered,
        message=message,
        error_type=error_type,
        error_code=error_code,
        error_detail=error_detail,
    )


# =============================================================================
# Actions - Sleep Workflow
# =============================================================================


@action
async def get_timestamp() -> str:
    """Get current timestamp as string."""
    from datetime import datetime

    return datetime.now().isoformat()


@action
async def format_sleep_result(started: str, resumed: str, seconds: int) -> SleepResult:
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
            return_exceptions=True,
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
    showing how waymark handles control flow.
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
class WhileLoopWorkflow(Workflow):
    """Demonstrates while-loop iteration with a counter."""

    async def run(self, limit: int) -> WhileLoopResult:
        current = 0
        iterations = 0

        for _ in range(limit):
            current = await increment_counter(current)
            iterations = iterations + 1

        return await build_while_result(
            limit=limit, final=current, iterations=iterations
        )


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

    Shows how waymark handles exceptions and allows
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


@action
async def risky_metadata_action(should_fail: bool) -> str:
    """Raise an exception with extra metadata to capture."""
    await asyncio.sleep(0.1)
    if should_fail:
        raise ExceptionMetadataError("Metadata error triggered", 418, "teapot")
    return "Metadata action completed"


@workflow
class ExceptionMetadataWorkflow(Workflow):
    """Demonstrate capturing exception metadata in the handler."""

    async def run(self, should_fail: bool) -> ErrorResult:
        recovered = False
        message = ""
        error_type = None
        error_code = None
        error_detail = None

        try:
            result = await self.run_action(
                risky_metadata_action(should_fail),
                retry=RetryPolicy(attempts=1),
            )
            message = await success_action(result)
        except ExceptionMetadataError as err:
            recovered = True
            error_type = "ExceptionMetadataError"
            error_code = err.code
            error_detail = err.detail
            message = await recovery_action("Captured exception metadata")

        return await build_error_result(
            True,
            recovered,
            message,
            error_type,
            error_code,
            error_detail,
        )


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
async def finalize_processing(
    items: list[str], processed_count: int
) -> EarlyReturnLoopResult:
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


@action
async def describe_location(request: KwOnlyLocationRequest) -> KwOnlyLocationResult:
    """Format a location from kw-only workflow inputs."""
    if request.latitude is None or request.longitude is None:
        return KwOnlyLocationResult(
            latitude=request.latitude,
            longitude=request.longitude,
            message="Location inputs are optional; provide both for a precise pin.",
        )
    return KwOnlyLocationResult(
        latitude=request.latitude,
        longitude=request.longitude,
        message=f"Resolved location at {request.latitude:.4f}, {request.longitude:.4f}.",
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


@workflow
class KwOnlyLocationWorkflow(Workflow):
    """
    Demonstrates kw-only inputs flowing into an action request model.

    Provides a safe default if latitude/longitude are omitted.
    """

    async def run(
        self,
        *,
        latitude: float | None = None,
        longitude: float | None = None,
    ) -> KwOnlyLocationResult:
        return await describe_location(
            KwOnlyLocationRequest(
                latitude=latitude,
                longitude=longitude,
            )
        )


global_fallback = "external-default"


@action
async def echo_external(value: str) -> str:
    """Echo a value to demonstrate undefined-variable validation."""
    return value


@workflow
class UndefinedVariableWorkflow(Workflow):
    """
    Demonstrates IR validation of references to out-of-scope variables.
    """

    async def run(self, input_text: str) -> str:
        return await echo_external(global_fallback)


# =============================================================================
# Actions - Loop with Exception Handling Workflow
# =============================================================================


class ItemProcessingError(Exception):
    """Exception raised when item processing fails."""

    pass


class LoopExceptionResult(BaseModel):
    """Result from the loop exception handling workflow."""

    items: list[str]
    processed: list[str]
    error_count: int
    message: str


class LoopExceptionRequest(BaseModel):
    items: list[str] = Field(
        min_length=1,
        max_length=10,
        description="Items to process. Items starting with 'bad' will fail.",
    )


@action
async def process_item_may_fail(item: str) -> str:
    """
    Process an item - fails for items starting with 'bad'.

    This simulates an action that may fail for certain inputs,
    demonstrating exception handling inside a for loop.
    """
    await asyncio.sleep(0.05)
    if item.lower().startswith("bad"):
        raise ItemProcessingError(f"Failed to process item: {item}")
    return f"processed:{item}"


@action
async def build_loop_exception_result(
    items: list[str],
    processed: list[str],
    error_count: int,
) -> LoopExceptionResult:
    """Build the final result with processed items and error count."""
    if error_count == 0:
        message = f"All {len(processed)} items processed successfully"
    elif error_count == len(items):
        message = f"All {error_count} items failed processing"
    else:
        message = f"Processed {len(processed)} items, {error_count} failures"
    return LoopExceptionResult(
        items=items,
        processed=processed,
        error_count=error_count,
        message=message,
    )


@workflow
class LoopExceptionWorkflow(Workflow):
    """
    Demonstrates exception handling inside a for loop.

    This workflow processes a list of items where some may fail.
    When an item fails:
    1. The exception is caught
    2. An error counter is incremented
    3. The loop continues to the next item

    This is a common pattern for batch processing where you want to
    continue processing remaining items even if some fail.

    Example inputs:
    - ["good1", "good2", "good3"] - All succeed, error_count=0
    - ["good1", "bad", "good2"] - One failure, error_count=1
    - ["bad1", "bad2"] - All fail, error_count=2
    """

    async def run(self, items: list[str]) -> LoopExceptionResult:
        processed: list[str] = []
        error_count = 0

        for item in items:
            try:
                result = await self.run_action(
                    process_item_may_fail(item),
                    retry=RetryPolicy(attempts=1),  # No retries, fail immediately
                )
                processed.append(result)
            except ItemProcessingError:
                # Increment error count and continue to next item
                error_count = error_count + 1

        return await build_loop_exception_result(items, processed, error_count)


# =============================================================================
# Actions - Spread Empty Collection Workflow
# =============================================================================


class SpreadEmptyResult(BaseModel):
    """Final result from the spread empty collection workflow."""

    items_processed: int
    message: str


class SpreadEmptyRequest(BaseModel):
    items: list[str] = Field(
        description="Items to process. Use empty list [] to test empty spread."
    )


@action
async def process_spread_item(item: str) -> str:
    """Process a single item in the spread."""
    await asyncio.sleep(0.05)
    return f"processed:{item}"


@action
async def build_spread_empty_result(
    results: list[str],
) -> SpreadEmptyResult:
    """Build the final result."""
    count = len(results)
    if count == 0:
        message = "No items to process - empty spread handled correctly!"
    else:
        message = f"Processed {count} items: {', '.join(results)}"
    return SpreadEmptyResult(
        items_processed=count,
        message=message,
    )


@workflow
class SpreadEmptyCollectionWorkflow(Workflow):
    """
    Demonstrates spread (parallel execution) over potentially empty collections.

    This workflow tests the scenario where asyncio.gather spreads over
    a collection that may be empty.

    The key test cases:
    - items=["a", "b", "c"] - Normal case with items
    - items=[] - Empty collection, should handle gracefully with no actions

    Before the fix, spreading over an empty collection would incorrectly
    dispatch actions. After the fix, empty spreads are handled correctly.
    """

    async def run(self, items: list[str]) -> SpreadEmptyResult:
        # Spread over items - may be empty!
        results = await asyncio.gather(
            *[process_spread_item(item=item) for item in items],
            return_exceptions=True,
        )

        return await build_spread_empty_result(results=results)


# =============================================================================
# No-op Workflow (queue throughput)
# =============================================================================


class NoOpRequest(BaseModel):
    """Inputs for the no-op queue benchmark workflow."""

    indices: list[int] = Field(
        default_factory=list,
        description="Indices to fan out over.",
    )
    complexity: int = Field(
        default=0,
        ge=0,
        description="Accepted for parity with benchmark inputs; unused.",
    )


class NoOpResult(BaseModel):
    """Summary for the no-op queue benchmark workflow."""

    count: int
    even_count: int
    odd_count: int


class NoOpTag(BaseModel):
    """Tagged value emitted by the no-op workflow."""

    value: int
    tag: str


@action
async def noop_int(value: int) -> int:
    """Return the input value without extra work."""
    return value


@action
async def noop_tag_from_value(value: int) -> NoOpTag:
    """Tag a value based on parity without extra work."""
    tag = "even" if value % 2 == 0 else "odd"
    return NoOpTag(value=value, tag=tag)


@action
async def noop_combine(items: list[NoOpTag]) -> NoOpResult:
    """Summarize tagged items with minimal work."""
    even_count = sum(1 for item in items if item.tag == "even")
    odd_count = len(items) - even_count
    return NoOpResult(count=len(items), even_count=even_count, odd_count=odd_count)


@workflow
class NoOpWorkflow(Workflow):
    """Queue stress workflow with fan-out, loops, and fan-in."""

    async def run(self, indices: list[int], complexity: int = 0) -> NoOpResult:
        _ = complexity

        stage1 = await asyncio.gather(
            *[noop_int(value=i) for i in indices],
            return_exceptions=True,
        )

        processed: list[int] = []
        for value in stage1:
            if value % 2 == 0:
                result = await noop_int(value=value)
            else:
                result = await noop_int(value=value)
            processed.append(result)

        tagged = await asyncio.gather(
            *[noop_tag_from_value(value=value) for value in processed],
            return_exceptions=True,
        )

        return await noop_combine(items=tagged)


# =============================================================================
# Many Actions Workflow (stress test)
# =============================================================================


class ManyActionsRequest(BaseModel):
    """Request for the many actions workflow."""

    action_count: int = Field(
        default=50,
        ge=1,
        description="Number of actions to execute",
    )
    parallel: bool = Field(
        default=True,
        description="Whether to run actions in parallel (True) or sequentially (False)",
    )


class ManyActionsResult(BaseModel):
    """Result from the many actions workflow."""

    action_count: int
    parallel: bool
    results: list[int]
    total: int


@action
async def compute_square(value: int) -> int:
    """Return a constant to keep the action as a no-op."""
    _ = value
    return 1


@action
async def aggregate_squares(
    squares: list[int], action_count: int, parallel: bool
) -> ManyActionsResult:
    """Aggregate the square computation results."""
    return ManyActionsResult(
        action_count=action_count,
        parallel=parallel,
        results=squares,
        total=sum(squares),
    )


@workflow
class ManyActionsWorkflow(Workflow):
    """
    Workflow that executes many actions to stress test the system.

    Can run actions in parallel (using asyncio.gather) or sequentially
    based on the `parallel` configuration parameter.
    """

    async def run(
        self, action_count: int = 50, parallel: bool = True
    ) -> ManyActionsResult:
        if parallel:
            # Fan out: run all actions in parallel
            results = await asyncio.gather(
                *[compute_square(value=i) for i in range(action_count)],
                return_exceptions=True,
            )
        else:
            # Run actions sequentially
            results = []
            for i in range(action_count):
                result = await compute_square(value=i)
                results.append(result)

        return await aggregate_squares(
            squares=results,
            action_count=action_count,
            parallel=parallel,
        )


# =============================================================================
# Looping Sleep Workflow (for testing durable sleep in loops)
# =============================================================================


class LoopingSleepRequest(BaseModel):
    """Request for looping sleep workflow."""

    iterations: int = Field(
        default=3, ge=1, le=100, description="Number of loop iterations"
    )
    sleep_seconds: int = Field(
        default=2, ge=1, le=60, description="Seconds to sleep each iteration"
    )


class LoopingSleepIteration(BaseModel):
    """Result of a single loop iteration."""

    iteration: int
    slept_seconds: int
    action_result: str
    timestamp: str


class LoopingSleepResult(BaseModel):
    """Final result from looping sleep workflow."""

    total_iterations: int
    total_sleep_seconds: int
    iterations: list[LoopingSleepIteration]


@action
async def perform_loop_action(iteration: int) -> str:
    """Perform an action within the loop iteration."""
    await asyncio.sleep(0.1)  # Small delay to simulate work
    return f"Processed iteration {iteration}"


@action
async def build_looping_sleep_result(
    total_iterations: int,
    total_sleep_seconds: int,
    iterations: list[LoopingSleepIteration],
) -> LoopingSleepResult:
    """Build the final looping sleep result."""
    return LoopingSleepResult(
        total_iterations=total_iterations,
        total_sleep_seconds=total_sleep_seconds,
        iterations=iterations,
    )


@workflow
class LoopingSleepWorkflow(Workflow):
    """
    Workflow that runs in a loop with sleep and action nodes.

    Each iteration:
    1. Sleeps for the specified duration (durable sleep)
    2. Performs an action
    3. Records the iteration result

    This is useful for testing looping sleep workflows and verifying
    that durable sleeps work correctly across multiple loop iterations.
    """

    async def run(
        self, iterations: int = 3, sleep_seconds: int = 2
    ) -> LoopingSleepResult:
        iteration_results: list[LoopingSleepIteration] = []

        for i in range(iterations):
            # Durable sleep
            await asyncio.sleep(sleep_seconds)

            # Perform action after sleep
            action_result = await perform_loop_action(iteration=i + 1)

            # Record iteration
            timestamp = await get_timestamp()
            iteration_results.append(
                LoopingSleepIteration(
                    iteration=i + 1,
                    slept_seconds=sleep_seconds,
                    action_result=action_result,
                    timestamp=timestamp,
                )
            )

        return await build_looping_sleep_result(
            total_iterations=iterations,
            total_sleep_seconds=iterations * sleep_seconds,
            iterations=iteration_results,
        )


# =============================================================================
# Legacy alias for backwards compatibility
# =============================================================================

ExampleMathWorkflow = ParallelMathWorkflow
