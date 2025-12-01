"""
Fan-out benchmark workflow without loops.

This benchmark is designed to test push-based scheduling by using only:
1. A setup action to generate work items
2. A fan-out phase (parallel processing of items)
3. A gather/finalize action to aggregate results

This avoids loops which require special handling in push-based scheduling.

Workflow Topology:
==================
1. SETUP: Generate work items based on fan_out_factor
2. FAN-OUT: Process each item in parallel
3. FINALIZE: Aggregate all results

Action Count Formula:
====================
- 1 (setup)
- fan_out_factor (parallel processing)
- 1 (finalize)

Example with fan_out=16: Total = 1 + 16 + 1 = 18 actions per workflow instance
Example with fan_out=32: Total = 1 + 32 + 1 = 34 actions per workflow instance
"""

import hashlib
import time

from pydantic import BaseModel

from rappel import Workflow, action, workflow


class WorkItem(BaseModel):
    """A single unit of work to process."""

    id: int
    payload: str
    iterations: int


class ProcessedItem(BaseModel):
    """Result of processing a work item."""

    id: int
    hash_result: str
    computation_result: int
    processing_time_ms: float


class FanoutResult(BaseModel):
    """Final results from the fan-out benchmark."""

    total_items_processed: int
    total_computation_result: int
    final_hash: str
    fan_out_factor: int


def cpu_intensive_work(iterations: int, seed: int) -> int:
    """Perform CPU-intensive work."""
    result = seed
    data = f"seed_{seed}_".encode()

    for i in range(iterations):
        result = (result * 31 + i) & 0xFFFFFFFF
        if i % 100 == 0:
            h = hashlib.sha256(data + result.to_bytes(4, "little")).digest()
            result ^= int.from_bytes(h[:4], "little")

    return result


def compute_hash(data: str, rounds: int = 10) -> str:
    """Compute iterated hash for non-trivial work."""
    result = data.encode()
    for _ in range(rounds):
        result = hashlib.sha256(result).digest()
    return result.hex()


@action(name="benchmark.fanout.generate_work")
async def generate_work(
    fan_out_factor: int, work_intensity: int, payload_size: int
) -> list[WorkItem]:
    """Generate work items for the fan-out phase."""
    payload = "x" * payload_size
    items = []
    for i in range(fan_out_factor):
        items.append(
            WorkItem(
                id=i,
                payload=payload,
                iterations=work_intensity,
            )
        )
    return items


@action(name="benchmark.fanout.process_item")
async def process_item(item: WorkItem) -> ProcessedItem:
    """Process a single work item with CPU-intensive work."""
    start = time.perf_counter()

    computation_result = cpu_intensive_work(item.iterations, item.id)
    hash_result = compute_hash(item.payload + str(item.id), rounds=5)

    elapsed_ms = (time.perf_counter() - start) * 1000

    return ProcessedItem(
        id=item.id,
        hash_result=hash_result,
        computation_result=computation_result,
        processing_time_ms=elapsed_ms,
    )


@action(name="benchmark.fanout.finalize")
async def finalize_results(
    processed_items: list[ProcessedItem], fan_out_factor: int
) -> FanoutResult:
    """Aggregate all processed items into final result."""
    total_computation = sum(item.computation_result for item in processed_items)
    combined_hashes = "".join(item.hash_result for item in processed_items)
    final_hash = compute_hash(combined_hashes, rounds=3)

    return FanoutResult(
        total_items_processed=len(processed_items),
        total_computation_result=total_computation,
        final_hash=final_hash,
        fan_out_factor=fan_out_factor,
    )


@workflow
class FanoutBenchmarkWorkflow(Workflow):
    """
    Simple fan-out benchmark workflow without loops.

    Parameters:
    - fan_out_factor: Number of parallel actions (default: 16)
    - work_intensity: CPU iterations per action (default: 1000)
    - payload_size: Data payload size in bytes (default: 1024)

    With defaults: 18 actions per workflow instance (1 + 16 + 1)
    """

    name = "benchmark.fanout"
    concurrent = True

    async def run(
        self,
        fan_out_factor: int = 16,
        work_intensity: int = 1000,
        payload_size: int = 1024,
    ) -> FanoutResult:
        # Phase 1: Generate work items
        work_items = await generate_work(
            fan_out_factor=fan_out_factor,
            work_intensity=work_intensity,
            payload_size=payload_size,
        )

        # Phase 2: Fan-out - process items in parallel
        processed_list: list[ProcessedItem] = []
        for item in work_items:
            result = await process_item(item)
            processed_list.append(result)

        # Phase 3: Finalize - aggregate results
        return await finalize_results(
            processed_items=processed_list,
            fan_out_factor=fan_out_factor,
        )
