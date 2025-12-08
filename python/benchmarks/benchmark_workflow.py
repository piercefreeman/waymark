"""
Benchmark workflow for stress testing the new durable execution engine.

This is similar to the old benchmark but designed for the new architecture:
- Uses @action decorator for durable actions
- Uses asyncio.gather for parallel fan-out
- Tests CPU-intensive workloads
"""

import asyncio
import hashlib
from typing import Any

from rappel import action, workflow, Workflow


@action
async def compute_hash_for_index(index: int, iterations: int) -> str:
    """Compute a hash chain for a given index."""
    seed = f"benchmark_seed_{index}".encode()
    result = seed
    for _ in range(iterations):
        result = hashlib.sha256(result).digest()
    return result.hex()


@action
async def analyze_hash(hash_value: str) -> dict[str, Any]:
    """Analyze a hash and return statistics."""
    leading_zeros = 0
    for c in hash_value:
        if c == "0":
            leading_zeros += 1
        else:
            break

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

    combined = "".join(results)
    final_hash = hashlib.sha256(combined.encode()).hexdigest()

    return {
        "total_processed": len(results),
        "special_count": special_count,
        "normal_count": normal_count,
        "final_hash": final_hash,
    }


@workflow
class BenchmarkFanOutWorkflow(Workflow):
    """
    Fan-out/fan-in benchmark with conditional processing.

    Demonstrates:
    1. Spread over a range (parallel fan-out)
    2. Sequential processing with conditional branching
    3. Final aggregation (fan-in)
    """

    async def run(
        self,
        indices: list[int],
        iterations: int = 100,
    ) -> dict[str, Any]:
        # Fan-out: compute hashes in parallel
        hashes = await asyncio.gather(
            *[compute_hash_for_index(index=i, iterations=iterations) for i in indices]
        )

        # Process each hash sequentially with conditional logic
        processed = []
        for hash_value in hashes:
            analysis = await analyze_hash(hash_value)

            if analysis["is_special"]:
                result = await process_special_hash(analysis)
            else:
                result = await process_normal_hash(analysis)

            processed.append(result)

        # Fan-in: combine all results
        summary = await combine_results(processed)
        return summary


@workflow
class SimpleFanOutWorkflow(Workflow):
    """
    Simpler fan-out only benchmark (no sequential processing).

    Just does parallel hash computation and aggregation.
    Useful for measuring pure parallel throughput.
    """

    async def run(
        self,
        count: int,
        iterations: int = 100,
    ) -> dict[str, Any]:
        # Fan-out: compute hashes in parallel
        hashes = await asyncio.gather(
            *[compute_hash_for_index(index=i, iterations=iterations) for i in range(count)]
        )

        # Simple aggregation
        combined = "".join(hashes)
        final_hash = hashlib.sha256(combined.encode()).hexdigest()

        return {
            "count": count,
            "final_hash": final_hash,
        }


@workflow
class BenchmarkWorkflow(Workflow):
    """
    Simple benchmark workflow for end-to-end testing.

    Used by the Rust rappel-benchmark binary. Just executes N hash actions
    sequentially to measure throughput.
    """

    async def run(
        self,
        count: int,
        iterations: int = 10,
    ) -> dict[str, Any]:
        # Execute count hash computations sequentially
        hashes = []
        for i in range(count):
            h = await compute_hash_for_index(index=i, iterations=iterations)
            hashes.append(h)

        # Simple aggregation
        combined = "".join(hashes)
        final_hash = hashlib.sha256(combined.encode()).hexdigest()

        return {
            "count": count,
            "final_hash": final_hash,
        }
