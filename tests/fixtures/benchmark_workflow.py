"""
Benchmark workflow for stress testing the Rappel runtime.

This workflow is designed to saturate the host CPU and test throughput:
1. Fan-out: Spawns many parallel action calls
2. CPU-intensive: Each action performs non-trivial computation
3. Fan-in: Aggregates results from all parallel branches
4. Nested parallelism: Multiple layers of fan-out/fan-in

Configurable parameters:
- fan_out_width: Number of parallel branches at each level
- computation_depth: CPU work per action (iterations)
- nesting_levels: Depth of nested fan-out/fan-in patterns
"""

import asyncio
import hashlib
from typing import Any

from rappel import action, workflow
from rappel.workflow import Workflow


# =============================================================================
# CPU-Intensive Actions
# =============================================================================


@action
async def hash_chain(seed: str, iterations: int) -> str:
    """
    Perform a chain of SHA-256 hashes.

    This is CPU-intensive and cannot be optimized away.
    Each iteration depends on the previous, preventing parallelization.
    """
    result = seed.encode()
    for _ in range(iterations):
        result = hashlib.sha256(result).digest()
    return result.hex()


@action
async def prime_sieve(limit: int) -> int:
    """
    Count primes up to limit using Sieve of Eratosthenes.

    CPU-intensive with memory pressure.
    """
    if limit < 2:
        return 0

    sieve = [True] * (limit + 1)
    sieve[0] = sieve[1] = False

    for i in range(2, int(limit ** 0.5) + 1):
        if sieve[i]:
            for j in range(i * i, limit + 1, i):
                sieve[j] = False

    return sum(sieve)


@action
async def matrix_multiply(size: int, seed: int) -> int:
    """
    Perform matrix multiplication on randomly seeded matrices.

    O(n^3) complexity - very CPU intensive for larger sizes.
    Returns a checksum of the result matrix.
    """
    # Simple PRNG for reproducible "random" matrices
    def lcg(x: int) -> int:
        return (1103515245 * x + 12345) & 0x7FFFFFFF

    # Generate matrices
    state = seed
    a: list[list[int]] = []
    b: list[list[int]] = []

    for i in range(size):
        row_a: list[int] = []
        row_b: list[int] = []
        for j in range(size):
            state = lcg(state)
            row_a.append(state % 100)
            state = lcg(state)
            row_b.append(state % 100)
        a.append(row_a)
        b.append(row_b)

    # Multiply
    c: list[list[int]] = [[0] * size for _ in range(size)]
    for i in range(size):
        for j in range(size):
            for k in range(size):
                c[i][j] += a[i][k] * b[k][j]

    # Checksum
    checksum = 0
    for row in c:
        for val in row:
            checksum ^= val
    return checksum


@action
async def fibonacci_memo(n: int) -> int:
    """
    Compute nth Fibonacci number with memoization.

    Tests memory allocation patterns with large dict.
    """
    memo: dict[int, int] = {0: 0, 1: 1}

    def fib(x: int) -> int:
        if x in memo:
            return memo[x]
        memo[x] = fib(x - 1) + fib(x - 2)
        return memo[x]

    return fib(n)


@action
async def string_processing(text: str, rounds: int) -> str:
    """
    Perform multiple rounds of string transformations.

    Tests string allocation and manipulation performance.
    """
    result = text
    for _ in range(rounds):
        # Reverse
        result = result[::-1]
        # Uppercase/lowercase alternating
        chars = []
        for i, c in enumerate(result):
            chars.append(c.upper() if i % 2 == 0 else c.lower())
        result = "".join(chars)
        # Hash and append
        h = hashlib.md5(result.encode()).hexdigest()[:8]
        result = result + h
    return result[-64:]  # Return last 64 chars to bound output size


# =============================================================================
# Aggregation Actions
# =============================================================================


@action
async def aggregate_hashes(hashes: list[str]) -> str:
    """Combine multiple hashes into a single result."""
    combined = "".join(hashes)
    return hashlib.sha256(combined.encode()).hexdigest()


@action
async def aggregate_counts(counts: list[int]) -> dict[str, Any]:
    """Aggregate numeric results with statistics."""
    if not counts:
        return {"sum": 0, "count": 0, "min": 0, "max": 0, "avg": 0.0}

    return {
        "sum": sum(counts),
        "count": len(counts),
        "min": min(counts),
        "max": max(counts),
        "avg": sum(counts) / len(counts),
    }


@action
async def final_summary(
    hash_result: str,
    prime_stats: dict[str, Any],
    matrix_checksum: int,
    fib_value: int,
    string_result: str,
) -> dict[str, Any]:
    """Create final benchmark summary."""
    return {
        "hash_aggregate": hash_result,
        "prime_statistics": prime_stats,
        "matrix_checksum": matrix_checksum,
        "fibonacci_result": fib_value,
        "string_sample": string_result,
        "benchmark_complete": True,
    }


# =============================================================================
# Additional Actions for Complex Workflow
# =============================================================================


@action
async def compute_hash_for_index(index: int, complexity: int) -> str:
    """Compute a hash chain for a given index.

    Args:
        index: The index to compute hash for
        complexity: Number of hash iterations (CPU intensity)
    """
    seed = ("benchmark_seed_" + str(index)).encode()
    result = seed
    for _ in range(complexity):
        result = hashlib.sha256(result).digest()
    return result.hex()


@action
async def noop_int(value: int) -> int:
    """Return the input value without extra work."""
    return value


@action
async def noop_tag(value: int, tag: str) -> dict[str, Any]:
    """Tag a value without extra work."""
    return {"value": value, "tag": tag}


@action
async def noop_tag_from_value(value: int) -> dict[str, Any]:
    """Tag a value based on parity without extra work."""
    if value % 2 == 0:
        tag = "even"
    else:
        tag = "odd"
    return {"value": value, "tag": tag}


@action
async def noop_combine(items: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize items with minimal work."""
    return {"count": len(items)}


@action
async def analyze_hash(hash_value: str) -> dict[str, Any]:
    """Analyze a hash and return statistics."""
    # Count leading zeros (simple analysis)
    leading_zeros = 0
    for c in hash_value:
        if c == "0":
            leading_zeros += 1
        else:
            break

    # Compute numeric value of first 8 chars
    prefix_value = int(hash_value[:8], 16)

    return {
        "hash": hash_value,
        "leading_zeros": leading_zeros,
        "prefix_value": prefix_value,
        "is_special": leading_zeros >= 2,
    }


@action
async def process_special_hash(analysis: dict[str, Any]) -> str:
    """Process a hash that was marked as special."""
    return "SPECIAL:" + analysis["hash"][:16]


@action
async def process_normal_hash(analysis: dict[str, Any]) -> str:
    """Process a hash that was not special."""
    return "NORMAL:" + analysis["hash"][:16]


@action
async def combine_results(results: list[str]) -> dict[str, Any]:
    """Combine all processed results into a final summary."""
    special_count = sum(1 for r in results if r.startswith("SPECIAL:"))
    normal_count = sum(1 for r in results if r.startswith("NORMAL:"))

    # Create a combined hash of all results
    combined = "".join(results)
    final_hash = hashlib.sha256(combined.encode()).hexdigest()

    return {
        "total_processed": len(results),
        "special_count": special_count,
        "normal_count": normal_count,
        "final_hash": final_hash,
    }


# =============================================================================
# Benchmark Workflows
# =============================================================================


@workflow
class BenchmarkFanOutWorkflow(Workflow):
    """
    Fan-out/fan-in benchmark with conditional processing (for-loop variant).

    This workflow demonstrates:
    1. Spread over a range (parallel fan-out)
    2. Sequential processing with conditional branching (blocking for loop)
    3. Final aggregation (fan-in)

    This tests the runtime's ability to handle sequential dependencies.

    Args:
        indices: List of indices to process (determines loop_size)
        complexity: CPU complexity per action (hash iterations)
    """

    async def run(
        self,
        indices: list[int],
        complexity: int = 100,
    ) -> dict[str, Any]:
        # Fan-out: compute hashes in parallel over the range
        hashes = await asyncio.gather(*[
            compute_hash_for_index(index=i, complexity=complexity)
            for i in indices
        ])

        # Process each hash sequentially with conditional logic
        processed = []
        for hash_value in hashes:
            # Analyze the hash
            analysis = await analyze_hash(hash_value)

            # Conditional processing based on analysis
            if analysis["is_special"]:
                result = await process_special_hash(analysis)
            else:
                result = await process_normal_hash(analysis)

            processed.append(result)

        # Fan-in: combine all results
        summary = await combine_results(processed)
        return summary


@workflow
class BenchmarkPureFanOutWorkflow(Workflow):
    """
    Pure fan-out benchmark - maximum parallelism test.

    This workflow tests maximum action completion parallelism by:
    1. Fanning out N actions that can all run in parallel
    2. Each action is independent (no sequential dependencies)
    3. Final aggregation waits for all results

    This tests the runtime's raw throughput when there are no
    sequential bottlenecks.

    Args:
        indices: List of indices to process (determines loop_size)
        complexity: CPU complexity per action (hash iterations)
    """

    async def run(
        self,
        indices: list[int],
        complexity: int = 100,
    ) -> dict[str, Any]:
        # Pure fan-out: compute ALL hashes in parallel
        hash_results = await asyncio.gather(*[
            compute_hash_for_index(index=i, complexity=complexity)
            for i in indices
        ])

        # Fan-in: aggregate with a single action (no sequential processing)
        # Note: hash_results from gather is already a list that can be passed directly
        final_hash = await aggregate_hashes(hashes=hash_results)

        return {
            "total_processed": len(hash_results),
            "final_hash": final_hash,
        }


@workflow
class BenchmarkQueueNoopWorkflow(Workflow):
    """
    Queue stress benchmark with minimal action work.

    This workflow exercises:
    1. Fan-out/fan-in barriers
    2. Sequential loop with conditional branches
    3. Additional fan-out aggregation

    Args:
        indices: List of indices to process (determines loop_size)
        complexity: Accepted for API parity, not used
    """

    async def run(
        self,
        indices: list[int],
        complexity: int = 0,
    ) -> dict[str, Any]:
        _ = complexity

        # Fan-out: noop actions in parallel
        stage1 = await asyncio.gather(*[
            noop_int(value=i)
            for i in indices
        ])

        # Sequential loop with conditional branch
        processed = []
        for value in stage1:
            if value % 2 == 0:
                result = await noop_int(value=value)
            else:
                result = await noop_int(value=value)
            processed.append(result)

        # Fan-out: tag values and fan-in summary
        tagged = await asyncio.gather(*[
            noop_tag_from_value(value=value)
            for value in processed
        ])

        summary = await noop_combine(items=tagged)
        return summary
