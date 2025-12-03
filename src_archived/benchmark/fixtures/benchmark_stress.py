"""
Stress test benchmark workflow designed to saturate CPU cores and test
maximum concurrent task handling.

Design principles:
1. Non-trivial action work (CPU-intensive but controllable)
2. Complex workflow topology (loops + gather + conditionals)
3. Parameterizable scale for different hardware

Workflow Topology:
==================
1. SETUP: Generate work items based on parameters
2. FAN-OUT: Process items in parallel using asyncio.gather
3. LOOP: Iterate over fan-out results, each iteration has multi-action body:
   - validate_chunk -> process_chunk -> aggregate_chunk
4. CONDITIONAL: Branch based on aggregated results (high/low path)
5. FAN-IN: Final aggregation and result formatting

Action Count Formula:
====================
- 1 (setup)
- fan_out_factor (parallel processing)
- loop_iterations * 3 (multi-action loop body)
- 1 (conditional branch)
- 1 (finalize)

Example with fan_out=16, loop_iterations=8:
Total = 1 + 16 + (8 * 3) + 1 + 1 = 43 actions per workflow instance

For maximum stress with fan_out=32, loop_iterations=32:
Total = 1 + 32 + (32 * 3) + 1 + 1 = 131 actions per workflow instance
"""

import asyncio
import hashlib
import os
import time
from dataclasses import dataclass

from pydantic import BaseModel

from rappel import Workflow, action, workflow


# =============================================================================
# Models
# =============================================================================


class WorkItem(BaseModel):
    """A single unit of work to process."""

    id: int
    payload: str
    iterations: int  # CPU work iterations


class ProcessedItem(BaseModel):
    """Result of processing a work item."""

    id: int
    hash_result: str
    computation_result: int
    processing_time_ms: float


class ChunkResult(BaseModel):
    """Result from processing a chunk in the loop."""

    chunk_id: int
    items_processed: int
    total_computation: int
    hash_digest: str


class ValidationResult(BaseModel):
    """Result from validating a chunk."""

    chunk_id: int
    is_valid: bool
    item_count: int
    checksum: int


class AggregationResult(BaseModel):
    """Result from aggregating chunk results."""

    chunk_id: int
    aggregated_hash: str
    total_items: int
    max_computation: int


class StressRunStats(BaseModel):
    """Final statistics from a stress benchmark run."""

    total_items_processed: int
    total_computation_result: int
    final_hash: str
    fan_out_actions: int
    loop_iterations: int
    actions_per_iteration: int
    total_actions: int
    branch_taken: str


# =============================================================================
# CPU-Intensive Work Functions (not actions, just computation)
# =============================================================================


def cpu_intensive_work(iterations: int, seed: int) -> int:
    """
    Perform CPU-intensive work that actually uses cycles.

    This is NOT an action - it's called within actions to do actual work.
    Uses a combination of:
    - Integer arithmetic
    - Hash computation
    - Memory operations
    """
    result = seed
    data = f"seed_{seed}_".encode()

    for i in range(iterations):
        # Mix of operations to prevent optimization
        result = (result * 31 + i) & 0xFFFFFFFF
        if i % 100 == 0:
            # Periodic hash computation to add real CPU work
            h = hashlib.sha256(data + result.to_bytes(4, "little")).digest()
            result ^= int.from_bytes(h[:4], "little")

    return result


def compute_hash(data: str, rounds: int = 10) -> str:
    """Compute iterated hash for non-trivial work."""
    result = data.encode()
    for _ in range(rounds):
        result = hashlib.sha256(result).digest()
    return result.hex()


# =============================================================================
# Actions - Setup Phase
# =============================================================================


@action(name="benchmark.stress.generate_work")
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


# =============================================================================
# Actions - Fan-Out Phase (Parallel Processing)
# =============================================================================


@action(name="benchmark.stress.process_item")
async def process_item(item: WorkItem) -> ProcessedItem:
    """
    Process a single work item with CPU-intensive work.

    This is the core action that does actual computation.
    """
    start = time.perf_counter()

    # Do CPU-intensive work
    computation_result = cpu_intensive_work(item.iterations, item.id)

    # Compute hash of payload
    hash_result = compute_hash(item.payload + str(item.id), rounds=5)

    elapsed_ms = (time.perf_counter() - start) * 1000

    return ProcessedItem(
        id=item.id,
        hash_result=hash_result,
        computation_result=computation_result,
        processing_time_ms=elapsed_ms,
    )


# =============================================================================
# Actions - Loop Phase (Multi-Action Loop Body)
# =============================================================================


@action(name="benchmark.stress.validate_chunk")
async def validate_chunk(chunk_id: int, items: list[ProcessedItem]) -> ValidationResult:
    """
    Validate a chunk of processed items.

    First action in the multi-action loop body.
    """
    # Do some validation work
    checksum = sum(item.computation_result for item in items)
    is_valid = all(len(item.hash_result) == 64 for item in items)

    return ValidationResult(
        chunk_id=chunk_id,
        is_valid=is_valid,
        item_count=len(items),
        checksum=checksum,
    )


@action(name="benchmark.stress.process_chunk")
async def process_chunk(
    chunk_id: int, validation: ValidationResult, items: list[ProcessedItem]
) -> ChunkResult:
    """
    Process a validated chunk with additional computation.

    Second action in the multi-action loop body.
    """
    # Additional computation based on validation
    total_computation = validation.checksum

    # Compute aggregate hash
    combined = "".join(item.hash_result for item in items)
    hash_digest = compute_hash(combined, rounds=3)

    return ChunkResult(
        chunk_id=chunk_id,
        items_processed=validation.item_count,
        total_computation=total_computation,
        hash_digest=hash_digest,
    )


@action(name="benchmark.stress.aggregate_chunk")
async def aggregate_chunk(
    chunk_id: int, chunk_result: ChunkResult, items: list[ProcessedItem]
) -> AggregationResult:
    """
    Aggregate chunk results for final processing.

    Third action in the multi-action loop body.
    """
    max_computation = max(item.computation_result for item in items)

    # Final hash combining chunk result with max computation
    aggregated_hash = compute_hash(
        chunk_result.hash_digest + str(max_computation), rounds=2
    )

    return AggregationResult(
        chunk_id=chunk_id,
        aggregated_hash=aggregated_hash,
        total_items=chunk_result.items_processed,
        max_computation=max_computation,
    )


# =============================================================================
# Actions - Conditional Branches
# =============================================================================


@action(name="benchmark.stress.high_path_finalize")
async def high_path_finalize(aggregations: list[AggregationResult]) -> str:
    """Handle high-value aggregation path."""
    # More intensive finalization for high values
    combined = "".join(agg.aggregated_hash for agg in aggregations)
    return compute_hash(combined, rounds=10)


@action(name="benchmark.stress.low_path_finalize")
async def low_path_finalize(aggregations: list[AggregationResult]) -> str:
    """Handle low-value aggregation path."""
    # Less intensive finalization for low values
    combined = "".join(agg.aggregated_hash for agg in aggregations)
    return compute_hash(combined, rounds=5)


# =============================================================================
# Actions - Final Aggregation
# =============================================================================


@action(name="benchmark.stress.finalize_results")
async def finalize_results(
    final_hash: str,
    branch_taken: str,
    fan_out_factor: int,
    loop_iterations: int,
    total_items: int,
    total_computation: int,
) -> StressRunStats:
    """Finalize and format the benchmark results."""
    actions_per_iteration = 3  # validate + process + aggregate
    total_actions = (
        1  # generate_work
        + fan_out_factor  # process_item (parallel)
        + (loop_iterations * actions_per_iteration)  # loop body
        + 1  # conditional branch
        + 1  # finalize
    )

    return StressRunStats(
        total_items_processed=total_items,
        total_computation_result=total_computation,
        final_hash=final_hash,
        fan_out_actions=fan_out_factor,
        loop_iterations=loop_iterations,
        actions_per_iteration=actions_per_iteration,
        total_actions=total_actions,
        branch_taken=branch_taken,
    )


# =============================================================================
# Workflow Definition
# =============================================================================


@workflow
class StressBenchmarkWorkflow(Workflow):
    """
    Stress test workflow with complex topology and CPU-intensive work.

    Parameters:
    - fan_out_factor: Number of parallel actions in gather (default: 16)
    - loop_iterations: Number of loop cycles with multi-action body (default: 8)
    - work_intensity: CPU iterations per action (default: 1000)
    - payload_size: Data payload size in bytes (default: 1024)

    With defaults: 43 actions per workflow instance
    Maximum stress (32, 32, 5000, 4096): 131 actions per instance
    """

    name = "benchmark.stress"
    concurrent = True

    async def run(
        self,
        fan_out_factor: int = 16,
        loop_iterations: int = 8,
        work_intensity: int = 1000,
        payload_size: int = 1024,
    ) -> StressRunStats:
        # =================================================================
        # Phase 1: Setup - Generate work items
        # =================================================================
        work_items = await generate_work(
            fan_out_factor=fan_out_factor,
            work_intensity=work_intensity,
            payload_size=payload_size,
        )

        # =================================================================
        # Phase 2: Fan-out - Process items in parallel
        # =================================================================
        # Process each work item (runs in parallel when concurrent=True)
        processed_list: list[ProcessedItem] = []
        for item in work_items:
            result = await process_item(item)
            processed_list.append(result)

        # =================================================================
        # Phase 3: Loop - Multi-action body over chunks
        # =================================================================
        # Divide processed items into chunks for loop iteration
        chunk_size = max(1, len(processed_list) // loop_iterations)
        aggregations: list[AggregationResult] = []

        for chunk_id in range(loop_iterations):
            # Get chunk of items (with wraparound for even distribution)
            start_idx = (chunk_id * chunk_size) % len(processed_list)
            chunk_items = []
            for j in range(chunk_size):
                idx = (start_idx + j) % len(processed_list)
                chunk_items.append(processed_list[idx])

            # Multi-action loop body: validate -> process -> aggregate
            validation = await validate_chunk(chunk_id=chunk_id, items=chunk_items)
            chunk_result = await process_chunk(
                chunk_id=chunk_id, validation=validation, items=chunk_items
            )
            aggregation = await aggregate_chunk(
                chunk_id=chunk_id, chunk_result=chunk_result, items=chunk_items
            )
            aggregations.append(aggregation)

        # =================================================================
        # Phase 4: Conditional - Branch based on total computation
        # =================================================================
        total_computation = sum(agg.max_computation for agg in aggregations)
        threshold = 0x7FFFFFFF  # Mid-point of uint32

        if total_computation > threshold:
            final_hash = await high_path_finalize(aggregations)
            branch_taken = "high"
        else:
            final_hash = await low_path_finalize(aggregations)
            branch_taken = "low"

        # =================================================================
        # Phase 5: Finalize - Aggregate and format results
        # =================================================================
        total_items = sum(agg.total_items for agg in aggregations)

        return await finalize_results(
            final_hash=final_hash,
            branch_taken=branch_taken,
            fan_out_factor=fan_out_factor,
            loop_iterations=loop_iterations,
            total_items=total_items,
            total_computation=total_computation,
        )


# =============================================================================
# Benchmark Runner (for local testing)
# =============================================================================


@dataclass
class StressBenchmarkResult:
    iterations: int
    fan_out_factor: int
    loop_iterations: int
    work_intensity: int
    payload_size: int
    elapsed: float
    total_actions: int

    @property
    def workflows_per_sec(self) -> float:
        if self.elapsed == 0:
            return 0.0
        return self.iterations / self.elapsed

    @property
    def actions_per_sec(self) -> float:
        if self.elapsed == 0:
            return 0.0
        return self.total_actions / self.elapsed


async def _run_stress_workflows(
    iterations: int,
    fan_out_factor: int,
    loop_iterations: int,
    work_intensity: int,
    payload_size: int,
) -> int:
    workflow = StressBenchmarkWorkflow()
    total_actions = 0
    for _ in range(iterations):
        stats = await workflow.run(
            fan_out_factor=fan_out_factor,
            loop_iterations=loop_iterations,
            work_intensity=work_intensity,
            payload_size=payload_size,
        )
        total_actions += stats.total_actions
    return total_actions


def run_stress_benchmark(
    iterations: int = 10,
    fan_out_factor: int = 16,
    loop_iterations: int = 8,
    work_intensity: int = 1000,
    payload_size: int = 1024,
) -> StressBenchmarkResult:
    """Execute the stress benchmark workflow repeatedly and measure throughput."""
    os.environ.setdefault("PYTEST_CURRENT_TEST", "benchmark_stress")
    start = time.perf_counter()
    total_actions = asyncio.run(
        _run_stress_workflows(
            iterations=iterations,
            fan_out_factor=fan_out_factor,
            loop_iterations=loop_iterations,
            work_intensity=work_intensity,
            payload_size=payload_size,
        )
    )
    elapsed = time.perf_counter() - start
    return StressBenchmarkResult(
        iterations=iterations,
        fan_out_factor=fan_out_factor,
        loop_iterations=loop_iterations,
        work_intensity=work_intensity,
        payload_size=payload_size,
        elapsed=elapsed,
        total_actions=total_actions,
    )
