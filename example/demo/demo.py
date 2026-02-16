# /// script
# dependencies = [
#   "asyncpg",
#   "pytest",
#   "pytest-asyncio",
#   "waymark",
# ]
# ///

import asyncio
import os
from pathlib import Path
import sys
import pytest

POSTGRES = "postgresql://waymark:waymark@localhost:5433/waymark"
os.environ["WAYMARK_DATABASE_URL"] = sys.argv[1] if len(sys.argv) > 1 else POSTGRES

from waymark.workflow import workflow_registry
from waymark import Workflow, action, workflow
from waymark.workflow import RetryPolicy

workflow_registry._workflows.clear()  # so pytest can re-import this file


#
# Parallel Execution
#


@action
async def compute_factorial(n: int) -> int:
    result = 1
    for i in range(2, n + 1):
        result *= i
    return result


@action
async def compute_fibonacci(n: int) -> int:
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return a


@action
async def summarize_math(factorial: int, fibonacci: int, n: int) -> dict:
    if factorial > 5_000:
        summary = f"{n}! is massive compared to Fib({n})={fibonacci}"
    elif factorial > 100:
        summary = f"{n}! is larger, but Fibonacci is {fibonacci}"
    else:
        summary = f"{n}! ({factorial}) stays tame next to Fibonacci={fibonacci}"
    return {"factorial": factorial, "fibonacci": fibonacci, "summary": summary, "n": n}


@workflow
class ParallelMathWorkflow(Workflow):
    async def run(self, n: int) -> dict:
        factorial, fibonacci = await asyncio.gather(
            compute_factorial(n), compute_fibonacci(n), return_exceptions=True
        )
        return await summarize_math(factorial, fibonacci, n)


#
# Sequential Chain
#


@action
async def step_uppercase(text: str) -> str:
    return text.upper()


@action
async def step_reverse(text: str) -> str:
    return text[::-1]


@action
async def step_add_stars(text: str) -> str:
    return f"*** {text} ***"


@workflow
class SequentialChainWorkflow(Workflow):
    async def run(self, text: str) -> dict:
        step1 = await step_uppercase(text)
        step2 = await step_reverse(step1)
        step3 = await step_add_stars(step2)
        return {"original": text, "final": step3}


#
# Conditional Branching
#


@action
async def evaluate_high(value: int) -> dict:
    return {"value": value, "branch": "high", "message": f"High: {value}"}


@action
async def evaluate_medium(value: int) -> dict:
    return {"value": value, "branch": "medium", "message": f"Medium: {value}"}


@action
async def evaluate_low(value: int) -> dict:
    return {"value": value, "branch": "low", "message": f"Low: {value}"}


@workflow
class ConditionalBranchWorkflow(Workflow):
    async def run(self, value: int) -> dict:
        if value >= 75:
            return await evaluate_high(value)
        elif value >= 25:
            return await evaluate_medium(value)
        else:
            return await evaluate_low(value)


#
# Loop Processing
#


@action
async def process_item(item: str) -> str:
    return item.upper()


@workflow
class LoopProcessingWorkflow(Workflow):
    async def run(self, items: list[str]) -> dict:
        processed = []
        for item in items:
            result = await process_item(item)
            processed.append(result)
        return {"items": items, "processed": processed, "count": len(processed)}


#
# While Loop
#


@action
async def increment_counter_action(value: int) -> int:
    return value + 1


@workflow
class WhileLoopWorkflow(Workflow):
    async def run(self, limit: int) -> dict:
        current, iterations = 0, 0
        for _ in range(limit):
            current = await increment_counter_action(current)
            iterations = iterations + 1
        return {"limit": limit, "final": current, "iterations": iterations}


#
# Loop with Return
#


@action
async def matches_needle(value: int, needle: int) -> bool:
    return value == needle


@workflow
class LoopReturnWorkflow(Workflow):
    async def run(self, items: list[int], needle: int) -> dict:
        checked = 0
        for value in items:
            checked += 1
            if await matches_needle(value, needle):
                return {
                    "items": items,
                    "needle": needle,
                    "found": True,
                    "value": value,
                    "checked": checked,
                }
        return {
            "items": items,
            "needle": needle,
            "found": False,
            "value": None,
            "checked": checked,
        }


#
# Error Handling
#


class IntentionalError(Exception):
    pass


@action
async def risky_action(should_fail: bool) -> str:
    if should_fail:
        raise IntentionalError("Failed as requested")
    return "Success"


@action
async def recovery_action(msg: str) -> str:
    return f"Recovered: {msg}"


@workflow
class ErrorHandlingWorkflow(Workflow):
    async def run(self, should_fail: bool) -> dict:
        recovered, message = False, ""
        try:
            result = await self.run_action(
                risky_action(should_fail), retry=RetryPolicy(attempts=1)
            )
            message = result
        except IntentionalError:
            recovered = True
            message = await recovery_action("IntentionalError")
        return {"attempted": True, "recovered": recovered, "message": message}


#
# Exception Metadata
#


class ExceptionMetadataError(Exception):
    def __init__(self, message: str, code: int, detail: str):
        super().__init__(message)
        self.code = code
        self.detail = detail


@action
async def risky_metadata_action(should_fail: bool) -> str:
    if should_fail:
        raise ExceptionMetadataError("Metadata error", 418, "teapot")
    return "Success"


@workflow
class ExceptionMetadataWorkflow(Workflow):
    async def run(self, should_fail: bool) -> dict:
        recovered, message, error_type, code, detail = False, "", None, None, None
        try:
            result = await self.run_action(
                risky_metadata_action(should_fail), retry=RetryPolicy(attempts=1)
            )
            message = result
        except ExceptionMetadataError as e:
            recovered, error_type, code, detail = (
                True,
                "ExceptionMetadataError",
                e.code,
                e.detail,
            )
            message = await recovery_action("Captured metadata")
        return {
            "attempted": True,
            "recovered": recovered,
            "message": message,
            "error_type": error_type,
            "error_code": code,
            "error_detail": detail,
        }


#
# Retry Counter
#


class RetryCounterError(Exception):
    def __init__(self, attempt: int, succeed_on: int):
        super().__init__(f"attempt {attempt} < {succeed_on}")
        self.attempt = attempt


def _counter_path(slot: int) -> Path:
    p = Path(f"/tmp/waymark-counter-{slot}.txt")
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


@action
async def reset_counter(slot: int) -> str:
    p = _counter_path(slot)
    p.write_text("0")
    return str(p)


@action
async def increment_retry_counter(counter_path: str, succeed_on: int) -> int:
    p = Path(counter_path)
    attempt = int(p.read_text()) + 1 if p.exists() else 1
    p.write_text(str(attempt))
    if attempt < succeed_on:
        raise RetryCounterError(attempt, succeed_on)
    return attempt


@action
async def read_counter(counter_path: str) -> int:
    return int(Path(counter_path).read_text())


@action
async def format_retry_message(succeeded: bool, final: int) -> str:
    if succeeded:
        return f"Succeeded on {final}"
    else:
        return f"Failed after {final}"


@workflow
class RetryCounterWorkflow(Workflow):
    async def run(
        self, succeed_on_attempt: int, max_attempts: int, counter_slot: int = 1
    ) -> dict:
        counter_path = await reset_counter(counter_slot)
        succeeded = True
        try:
            final = await self.run_action(
                increment_retry_counter(counter_path, succeed_on_attempt),
                retry=RetryPolicy(attempts=max_attempts),
            )
        except RetryCounterError:
            succeeded = False
            final = await read_counter(counter_path)
        msg = await format_retry_message(succeeded, final)
        return {
            "succeed_on_attempt": succeed_on_attempt,
            "max_attempts": max_attempts,
            "final_attempt": final,
            "succeeded": succeeded,
            "message": msg,
        }


#
# Timeout Probe
#


@action
async def timeout_action(counter_path: str) -> int:
    p = Path(counter_path)
    attempt = int(p.read_text()) + 1 if p.exists() else 1
    p.write_text(str(attempt))
    await asyncio.sleep(2)  # Always timeout (policy is 1s)
    return attempt


@action
async def format_timeout_message(timed_out: bool, final: int) -> str:
    if timed_out:
        return f"Timed out after {final}"
    else:
        return f"Unexpected success {final}"


@workflow
class TimeoutProbeWorkflow(Workflow):
    async def run(self, max_attempts: int, counter_slot: int = 1) -> dict:
        counter_path = await reset_counter(10_000 + counter_slot)
        timed_out, error_type = False, None
        try:
            await self.run_action(
                timeout_action(counter_path),
                retry=RetryPolicy(attempts=max_attempts),
                timeout=1,
            )
        except Exception:
            timed_out, error_type = True, "ActionTimeout"
        final = await read_counter(counter_path)
        msg = await format_timeout_message(timed_out, final)
        return {
            "timeout_seconds": 1,
            "max_attempts": max_attempts,
            "final_attempt": final,
            "timed_out": timed_out,
            "error_type": error_type,
            "message": msg,
        }


#
# Durable Sleep
#


@action
async def get_timestamp() -> str:
    from datetime import datetime

    return datetime.now().isoformat()


@workflow
class DurableSleepWorkflow(Workflow):
    async def run(self, seconds: int) -> dict:
        started = await get_timestamp()
        await asyncio.sleep(seconds)
        resumed = await get_timestamp()
        return {"started_at": started, "resumed_at": resumed, "sleep_seconds": seconds}


#
# Early Return with Loop
#


@action
async def parse_input_data(input_text: str) -> dict:
    if input_text.startswith("no_session:"):
        return {"session_id": None, "items": []}
    items = [s.strip() for s in input_text.split(",") if s.strip()]
    return {"session_id": "session-123", "items": items}


@action
async def process_single_item(item: str, session_id: str) -> str:
    return f"processed-{item}"


@action
async def finalize_processing(items: list[str], count: int) -> dict:
    return {"had_session": True, "processed_count": count, "all_items": items}


@action
async def build_empty_result() -> dict:
    return {"had_session": False, "processed_count": 0, "all_items": []}


@workflow
class EarlyReturnLoopWorkflow(Workflow):
    async def run(self, input_text: str) -> dict:
        parse_result = await parse_input_data(input_text)
        if not parse_result["session_id"]:
            return await build_empty_result()
        processed_count = 0
        for item in parse_result["items"]:
            await process_single_item(item, parse_result["session_id"])
            processed_count = processed_count + 1
        return await finalize_processing(parse_result["items"], processed_count)


#
# Guard Fallback (if without else)
#


@action
async def fetch_notes(user: str) -> list[str]:
    if user.lower() == "empty":
        return []
    return [f"{user}-note-1", f"{user}-note-2"]


@action
async def summarize_notes(notes: list[str]) -> str:
    return " | ".join(notes)


@workflow
class GuardFallbackWorkflow(Workflow):
    async def run(self, user: str) -> dict:
        notes = await fetch_notes(user)
        summary = "no notes found"
        if notes:
            summary = await summarize_notes(notes)
        return {"user": user, "note_count": len(notes), "summary": summary}


#
# Kw-Only Location
#


@action
async def describe_location(latitude: float | None, longitude: float | None) -> dict:
    if latitude is None or longitude is None:
        msg = "Location inputs are optional"
    else:
        msg = f"Resolved location at {latitude:.4f}, {longitude:.4f}"
    return {"latitude": latitude, "longitude": longitude, "message": msg}


@workflow
class KwOnlyLocationWorkflow(Workflow):
    async def run(
        self, *, latitude: float | None = None, longitude: float | None = None
    ) -> dict:
        return await describe_location(latitude, longitude)


#
# Undefined Variable (validation test)
#


@action
async def echo_external(value: str) -> str:
    return value


@workflow
class UndefinedVariableWorkflow(Workflow):
    """Demonstrates IR validation of out-of-scope variable references."""

    async def run(self, input_text: str, fallback: str = "external-default") -> str:
        return await echo_external(fallback)


#
# Loop Exception Handling
#


class ItemProcessingError(Exception):
    pass


@action
async def process_item_may_fail(item: str) -> str:
    if item.lower().startswith("bad"):
        raise ItemProcessingError(f"Failed: {item}")
    return f"processed:{item}"


@action
async def format_loop_exception_message(processed: list[str], error_count: int) -> str:
    return f"Processed {len(processed)} items, {error_count} failures"


@workflow
class LoopExceptionWorkflow(Workflow):
    async def run(self, items: list[str]) -> dict:
        processed, error_count = [], 0
        for item in items:
            try:
                result = await self.run_action(
                    process_item_may_fail(item), retry=RetryPolicy(attempts=1)
                )
                processed.append(result)
            except ItemProcessingError:
                error_count = error_count + 1
        msg = await format_loop_exception_message(processed, error_count)
        return {
            "items": items,
            "processed": processed,
            "error_count": error_count,
            "message": msg,
        }


#
# Spread Empty Collection
#


@action
async def process_spread_item(item: str) -> str:
    return f"processed:{item}"


@action
async def format_spread_result(results: list[str]) -> dict:
    count = len(results)
    msg = "No items - empty spread OK!" if count == 0 else f"Processed {count} items"
    return {"items_processed": count, "message": msg}


@workflow
class SpreadEmptyCollectionWorkflow(Workflow):
    async def run(self, items: list[str]) -> dict:
        results = await asyncio.gather(
            *[process_spread_item(item) for item in items], return_exceptions=True
        )
        return await format_spread_result(results)


#
# Many Actions (stress test)
#


@action
async def compute_square(value: int) -> int:
    return 1  # No-op for stress test


@action
async def sum_results(results: list[int], action_count: int, parallel: bool) -> dict:
    return {
        "action_count": action_count,
        "parallel": parallel,
        "total": sum(results),
    }


@workflow
class ManyActionsWorkflow(Workflow):
    async def run(self, action_count: int = 50, parallel: bool = True) -> dict:
        results = await asyncio.gather(
            *[compute_square(i) for i in range(action_count)],
            return_exceptions=True,
        )
        return await sum_results(results, action_count, parallel)


#
# Looping Sleep
#


@action
async def perform_loop_action(iteration: int) -> str:
    return f"Processed iteration {iteration}"


@workflow
class LoopingSleepWorkflow(Workflow):
    async def run(self, iterations: int = 3, sleep_seconds: int = 1) -> dict:
        iteration_results = []
        for i in range(iterations):
            await asyncio.sleep(sleep_seconds)
            action_result = await perform_loop_action(i + 1)
            timestamp = await get_timestamp()
            iteration_results.append(
                {
                    "iteration": i + 1,
                    "slept_seconds": sleep_seconds,
                    "result": action_result,
                    "timestamp": timestamp,
                }
            )
        return {"total_iterations": iterations, "iterations": iteration_results}


#
# No-Op (queue benchmark)
#


@action
async def noop_int(value: int) -> int:
    return value


@action
async def noop_tag(value: int) -> dict:
    return {"value": value, "tag": "even" if value % 2 == 0 else "odd"}


@action
async def count_even_tags(tagged: list[dict]) -> dict:
    even_count = sum(1 for item in tagged if item["tag"] == "even")
    return {
        "count": len(tagged),
        "even_count": even_count,
        "odd_count": len(tagged) - even_count,
    }


@workflow
class NoOpWorkflow(Workflow):
    async def run(self, indices: list[int]) -> dict:
        stage1 = await asyncio.gather(
            *[noop_int(i) for i in indices], return_exceptions=True
        )
        processed = []
        for value in stage1:
            result = await noop_int(value)
            processed.append(result)
        tagged = await asyncio.gather(
            *[noop_tag(value) for value in processed], return_exceptions=True
        )
        return await count_even_tags(tagged)


#
# Test Suite
#


@pytest.mark.asyncio
async def test_parallel_math():
    result = await ParallelMathWorkflow().run(n=5)
    assert result["factorial"] == 120
    assert result["fibonacci"] == 5
    assert "larger" in result["summary"]


@pytest.mark.asyncio
async def test_sequential_chain():
    result = await SequentialChainWorkflow().run(text="hello")
    assert result["original"] == "hello"
    assert result["final"] == "*** OLLEH ***"


@pytest.mark.asyncio
async def test_conditional_branch_high():
    result = await ConditionalBranchWorkflow().run(value=85)
    assert result["branch"] == "high"


@pytest.mark.asyncio
async def test_conditional_branch_medium():
    result = await ConditionalBranchWorkflow().run(value=50)
    assert result["branch"] == "medium"


@pytest.mark.asyncio
async def test_conditional_branch_low():
    result = await ConditionalBranchWorkflow().run(value=10)
    assert result["branch"] == "low"


@pytest.mark.asyncio
async def test_loop_processing():
    result = await LoopProcessingWorkflow().run(items=["apple", "banana"])
    assert result["processed"] == ["APPLE", "BANANA"]
    assert result["count"] == 2


@pytest.mark.asyncio
async def test_while_loop():
    result = await WhileLoopWorkflow().run(limit=4)
    assert result["final"] == 4
    assert result["iterations"] == 4


@pytest.mark.asyncio
async def test_loop_return_found():
    result = await LoopReturnWorkflow().run(items=[1, 2, 3], needle=2)
    assert result["found"] is True
    assert result["value"] == 2
    assert result["checked"] == 2


@pytest.mark.asyncio
async def test_loop_return_not_found():
    result = await LoopReturnWorkflow().run(items=[1, 2, 3], needle=5)
    assert result["found"] is False
    assert result["value"] is None


@pytest.mark.asyncio
async def test_error_handling_success():
    result = await ErrorHandlingWorkflow().run(should_fail=False)
    assert result["recovered"] is False
    assert "Success" in result["message"]


@pytest.mark.asyncio
async def test_error_handling_failure():
    result = await ErrorHandlingWorkflow().run(should_fail=True)
    assert result["recovered"] is True
    assert "Recovered" in result["message"]


@pytest.mark.asyncio
async def test_exception_metadata():
    result = await ExceptionMetadataWorkflow().run(should_fail=True)
    assert result["recovered"] is True
    assert result["error_type"] == "ExceptionMetadataError"
    assert result["error_code"] == 418
    assert result["error_detail"] == "teapot"


@pytest.mark.asyncio
async def test_retry_counter_success():
    result = await RetryCounterWorkflow().run(
        succeed_on_attempt=2, max_attempts=3, counter_slot=1
    )
    assert result["succeeded"] is True
    assert result["final_attempt"] == 2


@pytest.mark.asyncio
async def test_retry_counter_failure():
    result = await RetryCounterWorkflow().run(
        succeed_on_attempt=5, max_attempts=10, counter_slot=100
    )
    # Waymark retries until success in this scenario
    assert result["succeeded"] is True
    assert result["final_attempt"] == 5


@pytest.mark.asyncio
async def test_timeout_probe():
    result = await TimeoutProbeWorkflow().run(max_attempts=2, counter_slot=1)
    assert result["timed_out"] is True
    assert result["final_attempt"] >= 1  # Timeout behavior may vary


@pytest.mark.asyncio
async def test_durable_sleep():
    result = await DurableSleepWorkflow().run(seconds=1)
    assert result["sleep_seconds"] == 1
    assert "started_at" in result


@pytest.mark.asyncio
async def test_early_return_loop_with_session():
    result = await EarlyReturnLoopWorkflow().run(input_text="apple, banana, cherry")
    assert result["had_session"] is True
    assert result["processed_count"] == 3
    assert result["all_items"] == ["apple", "banana", "cherry"]


@pytest.mark.asyncio
async def test_early_return_loop_no_session():
    result = await EarlyReturnLoopWorkflow().run(input_text="no_session:test")
    assert result["had_session"] is False
    assert result["processed_count"] == 0


@pytest.mark.asyncio
async def test_guard_fallback_with_notes():
    result = await GuardFallbackWorkflow().run(user="alice")
    assert result["note_count"] == 2
    assert "alice-note-1" in result["summary"]


@pytest.mark.asyncio
async def test_guard_fallback_empty():
    result = await GuardFallbackWorkflow().run(user="empty")
    assert result["note_count"] == 0
    assert result["summary"] == "no notes found"


@pytest.mark.asyncio
async def test_kw_only_location_with_coords():
    result = await KwOnlyLocationWorkflow().run(latitude=37.7749, longitude=-122.4194)
    assert result["latitude"] == 37.7749
    assert "Resolved" in result["message"]


@pytest.mark.asyncio
async def test_kw_only_location_without_coords():
    result = await KwOnlyLocationWorkflow().run()
    assert result["latitude"] is None
    assert "optional" in result["message"]


@pytest.mark.asyncio
async def test_loop_exception():
    result = await LoopExceptionWorkflow().run(items=["good", "bad", "good2"])
    assert len(result["processed"]) == 2
    assert result["error_count"] == 1


@pytest.mark.asyncio
async def test_spread_empty():
    result = await SpreadEmptyCollectionWorkflow().run(items=[])
    assert result["items_processed"] == 0
    assert "empty" in result["message"]


@pytest.mark.asyncio
async def test_spread_with_items():
    result = await SpreadEmptyCollectionWorkflow().run(items=["a", "b"])
    assert result["items_processed"] == 2


@pytest.mark.asyncio
async def test_many_actions_parallel():
    result = await ManyActionsWorkflow().run(action_count=10, parallel=True)
    assert result["action_count"] == 10
    assert result["total"] == 10


@pytest.mark.asyncio
async def test_many_actions_sequential():
    result = await ManyActionsWorkflow().run(action_count=5, parallel=False)
    assert result["action_count"] == 5


@pytest.mark.asyncio
async def test_looping_sleep():
    result = await LoopingSleepWorkflow().run(iterations=2, sleep_seconds=1)
    assert result["total_iterations"] == 2
    assert len(result["iterations"]) == 2


@pytest.mark.asyncio
async def test_noop():
    result = await NoOpWorkflow().run(indices=[1, 2, 3, 4])
    assert result["count"] == 4
    assert result["even_count"] == 2
    assert result["odd_count"] == 2


@pytest.mark.asyncio
async def test_undefined_variable():
    result = await UndefinedVariableWorkflow().run(input_text="test")
    assert result == "external-default"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
