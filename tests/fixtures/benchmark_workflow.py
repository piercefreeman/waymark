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
# Benchmark Workflows
# =============================================================================


@workflow
class BenchmarkFanOutWorkflow(Workflow):
    """
    Simple fan-out/fan-in benchmark.

    Spawns `width` parallel hash chains, then aggregates results.
    """

    async def run(
        self,
        width: int = 10,
        hash_iterations: int = 1000,
    ) -> str:
        # Fan-out: spawn parallel hash computations
        tasks = []
        for i in range(width):
            tasks.append(hash_chain(seed=f"bench_{i}", iterations=hash_iterations))

        # Execute in parallel
        results = await asyncio.gather(*tasks)

        # Fan-in: aggregate results
        final = await aggregate_hashes(list(results))
        return final


@workflow
class BenchmarkMixedWorkflow(Workflow):
    """
    Mixed workload benchmark with different computation types.

    Tests heterogeneous parallel execution where actions have
    different CPU profiles (hash-bound, memory-bound, etc.)
    """

    async def run(
        self,
        hash_count: int = 5,
        hash_iterations: int = 500,
        prime_limit: int = 10000,
        matrix_size: int = 50,
        fib_n: int = 100,
        string_rounds: int = 20,
    ) -> dict[str, Any]:
        # Layer 1: Fan-out different computation types

        # Hash chains (CPU bound, predictable)
        hash_tasks = []
        for i in range(hash_count):
            hash_tasks.append(hash_chain(seed=f"mixed_{i}", iterations=hash_iterations))

        # Prime counting (CPU + memory)
        prime_task = prime_sieve(limit=prime_limit)

        # Matrix multiply (CPU intensive, O(n^3))
        matrix_task = matrix_multiply(size=matrix_size, seed=42)

        # Fibonacci (recursion + memoization)
        fib_task = fibonacci_memo(n=fib_n)

        # String processing (allocation heavy)
        string_task = string_processing(
            text="benchmark_test_string_for_processing",
            rounds=string_rounds,
        )

        # Execute all in parallel
        hash_results, prime_count, matrix_sum, fib_result, string_result = await asyncio.gather(
            asyncio.gather(*hash_tasks),
            prime_task,
            matrix_task,
            fib_task,
            string_task,
        )

        # Layer 2: Fan-in aggregation
        hash_aggregate = await aggregate_hashes(list(hash_results))
        prime_stats = await aggregate_counts([prime_count])

        # Final summary
        result = await final_summary(
            hash_result=hash_aggregate,
            prime_stats=prime_stats,
            matrix_checksum=matrix_sum,
            fib_value=fib_result,
            string_result=string_result,
        )
        return result


@workflow
class BenchmarkNestedFanOutWorkflow(Workflow):
    """
    Nested fan-out/fan-in benchmark for testing deep parallelism.

    Creates a tree of parallel computations:
    - Level 0: Spawn N branches
    - Level 1: Each branch spawns M sub-computations
    - Results aggregate up the tree
    """

    async def run(
        self,
        branches: int = 4,
        sub_branches: int = 4,
        hash_iterations: int = 500,
        prime_limit: int = 5000,
    ) -> dict[str, Any]:
        # Level 1: Create branch tasks
        branch_results = []

        for branch_id in range(branches):
            # Each branch does parallel work
            # Sub-fan-out: hash chains
            hash_tasks = []
            for sub_id in range(sub_branches):
                hash_tasks.append(
                    hash_chain(
                        seed=f"branch_{branch_id}_sub_{sub_id}",
                        iterations=hash_iterations,
                    )
                )

            # Sub-fan-out: prime counts with different limits
            prime_tasks = []
            for sub_id in range(sub_branches):
                limit = prime_limit + (sub_id * 1000)
                prime_tasks.append(prime_sieve(limit=limit))

            # Execute this branch's parallel work
            hash_results, prime_results = await asyncio.gather(
                asyncio.gather(*hash_tasks),
                asyncio.gather(*prime_tasks),
            )

            # Aggregate this branch
            branch_hash = await aggregate_hashes(list(hash_results))
            branch_primes = await aggregate_counts(list(prime_results))

            branch_results.append({
                "branch_id": branch_id,
                "hash": branch_hash,
                "primes": branch_primes,
            })

        # Final aggregation across all branches
        all_hashes = [b["hash"] for b in branch_results]
        final_hash = await aggregate_hashes(all_hashes)

        total_primes = sum(b["primes"]["sum"] for b in branch_results)

        return {
            "branches": branches,
            "sub_branches": sub_branches,
            "total_actions": branches * sub_branches * 2 + branches * 2 + 1,
            "final_hash": final_hash,
            "total_primes_found": total_primes,
            "branch_details": branch_results,
        }


@workflow
class BenchmarkStressTestWorkflow(Workflow):
    """
    Maximum stress test workflow.

    Designed to saturate all available CPU cores with:
    - High fan-out width
    - CPU-intensive computations per action
    - Multiple aggregation layers
    - Mixed computation types
    """

    async def run(
        self,
        parallelism: int = 16,
        intensity: int = 2,  # Multiplier for computation depth
    ) -> dict[str, Any]:
        # Scale parameters based on intensity
        hash_iters = 1000 * intensity
        prime_limit = 10000 * intensity
        matrix_size = 30 + (10 * intensity)
        fib_n = 50 + (25 * intensity)
        string_rounds = 10 * intensity

        # Wave 1: Pure hash chains (predictable CPU load)
        wave1_tasks = []
        for i in range(parallelism):
            wave1_tasks.append(
                hash_chain(seed=f"stress_wave1_{i}", iterations=hash_iters)
            )
        wave1_results = await asyncio.gather(*wave1_tasks)
        wave1_hash = await aggregate_hashes(list(wave1_results))

        # Wave 2: Mixed compute (heterogeneous load)
        wave2_primes = []
        wave2_matrices = []
        for i in range(parallelism // 2):
            wave2_primes.append(prime_sieve(limit=prime_limit + i * 500))
            wave2_matrices.append(matrix_multiply(size=matrix_size, seed=i))

        prime_results, matrix_results = await asyncio.gather(
            asyncio.gather(*wave2_primes),
            asyncio.gather(*wave2_matrices),
        )

        prime_stats = await aggregate_counts(list(prime_results))
        matrix_checksum = sum(matrix_results) & 0xFFFFFFFF

        # Wave 3: Memory-intensive operations
        wave3_tasks = []
        for i in range(parallelism // 4):
            wave3_tasks.append(fibonacci_memo(n=fib_n + i * 10))
            wave3_tasks.append(
                string_processing(
                    text=f"stress_test_string_{i}" * 10,
                    rounds=string_rounds,
                )
            )
        wave3_results = await asyncio.gather(*wave3_tasks)

        # Separate fib and string results
        fib_results = [r for r in wave3_results if isinstance(r, int)]
        string_results = [r for r in wave3_results if isinstance(r, str)]

        fib_sum = sum(fib_results) if fib_results else 0
        string_sample = string_results[0] if string_results else ""

        # Final aggregation
        return await final_summary(
            hash_result=wave1_hash,
            prime_stats=prime_stats,
            matrix_checksum=matrix_checksum,
            fib_value=fib_sum,
            string_result=string_sample,
        )
