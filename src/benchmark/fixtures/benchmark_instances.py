"""Workflow-oriented benchmark fixtures."""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass

from carabiner_worker import Workflow, action, workflow

from .benchmark_common import InstanceRunStats, build_requests, summarize_payload


@action(name="benchmark.instances.prepare_requests")
async def prepare_requests(batch_size: int, payload_size: int) -> list[str]:
    requests = build_requests(batch_size, payload_size)
    return [request.payload for request in requests]


@action(name="benchmark.instances.summarize")
async def summarize_requests(requests: list[str]) -> InstanceRunStats:
    summaries = [summarize_payload(payload) for payload in requests]
    total_bytes = sum(summary.length for summary in summaries)
    checksum = sum(summary.checksum for summary in summaries)
    return InstanceRunStats(responses=len(summaries), checksum=checksum, total_bytes=total_bytes)


@action(name="benchmark.instances.persist_results")
async def persist_results(results: InstanceRunStats) -> InstanceRunStats:
    return results


@workflow
class BenchmarkInstancesWorkflow(Workflow):
    """Workflow that exercises action dispatch for benchmarking."""

    name = "benchmark.instances"
    concurrent = True

    async def run(self, batch_size: int = 4, payload_size: int = 1024) -> InstanceRunStats:
        requests = await prepare_requests(batch_size=batch_size, payload_size=payload_size)
        summary = await summarize_requests(requests=requests)
        return await persist_results(results=summary)


@dataclass
class InstancesBenchmarkResult:
    iterations: int
    batch_size: int
    payload_size: int
    elapsed: float
    responses: int

    @property
    def workflows_per_sec(self) -> float:
        if self.elapsed == 0:
            return 0.0
        return self.iterations / self.elapsed

    @property
    def actions_per_sec(self) -> float:
        if self.elapsed == 0:
            return 0.0
        total_actions = self.responses + (self.iterations * 2)
        return total_actions / self.elapsed


async def _run_workflows(iterations: int, batch_size: int, payload_size: int) -> int:
    workflow = BenchmarkInstancesWorkflow()
    responses = 0
    for _ in range(iterations):
        stats = await workflow.run(batch_size=batch_size, payload_size=payload_size)
        responses += stats.responses
    return responses


def run_benchmark_instances(
    iterations: int = 10,
    batch_size: int = 4,
    payload_size: int = 1024,
) -> InstancesBenchmarkResult:
    """Execute the benchmark workflow repeatedly and measure throughput."""

    os.environ.setdefault("PYTEST_CURRENT_TEST", "benchmark_instances")
    start = time.perf_counter()
    responses = asyncio.run(_run_workflows(iterations, batch_size, payload_size))
    elapsed = time.perf_counter() - start
    return InstancesBenchmarkResult(
        iterations=iterations,
        batch_size=batch_size,
        payload_size=payload_size,
        elapsed=elapsed,
        responses=responses,
    )

