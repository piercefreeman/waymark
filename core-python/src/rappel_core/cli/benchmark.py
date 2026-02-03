"""Benchmark CLI for running mixed IR workloads against Postgres."""

from __future__ import annotations

import argparse
import asyncio
import random
from dataclasses import dataclass
from typing import Any

from pyinstrument import Profiler
from proto import ast_pb2 as ir

from ..backends import PostgresBackend, QueuedInstance
from ..dag import DAG, convert_to_dag
from ..ir_examples import EXAMPLES
from ..runloop import RunLoop
from ..runner import RunnerState
from ..workers import InlineWorkerPool
from .smoke import _build_program


async def _action_double(value: int) -> int:
    return value * 2


async def _action_sum(values: list[int]) -> int:
    return sum(values)


ACTION_REGISTRY = {
    "double": _action_double,
    "sum": _action_sum,
}


@dataclass(frozen=True)
class BenchmarkCase:
    name: str
    program: ir.Program
    dag: DAG
    inputs: dict[str, Any]


def _literal_from_value(value: Any) -> ir.Expr:
    if isinstance(value, bool):
        return ir.Expr(literal=ir.Literal(bool_value=value))
    if isinstance(value, int):
        return ir.Expr(literal=ir.Literal(int_value=value))
    if isinstance(value, float):
        return ir.Expr(literal=ir.Literal(float_value=value))
    if isinstance(value, str):
        return ir.Expr(literal=ir.Literal(string_value=value))
    if isinstance(value, (list, tuple)):
        return ir.Expr(list=ir.ListExpr(elements=[_literal_from_value(item) for item in value]))
    if isinstance(value, dict):
        entries = []
        for key, item in value.items():
            key_expr = _literal_from_value(key)
            entries.append(ir.DictEntry(key=key_expr, value=_literal_from_value(item)))
        return ir.Expr(dict=ir.DictExpr(entries=entries))
    if value is None:
        return ir.Expr(literal=ir.Literal(is_none=True))
    raise ValueError(f"unsupported input literal: {value!r}")


def _build_cases(base: int) -> dict[str, BenchmarkCase]:
    smoke_program = _build_program()
    cases = {
        "smoke": (smoke_program, {"base": base}),
        "control_flow": (EXAMPLES["control_flow"](), {"base": 2}),
        "parallel_spread": (EXAMPLES["parallel_spread"](), {"base": 3}),
        "try_except": (EXAMPLES["try_except"](), {"values": [1, 2, 3]}),
        "while_loop": (EXAMPLES["while_loop"](), {"limit": 6}),
    }
    return {
        name: BenchmarkCase(
            name=name,
            program=program,
            dag=convert_to_dag(program),
            inputs=inputs,
        )
        for name, (program, inputs) in cases.items()
    }


def _build_instance(case: BenchmarkCase) -> QueuedInstance:
    state = RunnerState(dag=case.dag, link_queued_nodes=False)
    for name, value in case.inputs.items():
        state.record_assignment(
            targets=[name],
            expr=_literal_from_value(value),
            label=f"input {name} = {value!r}",
        )
    if case.dag.entry_node is None:
        raise RuntimeError(f"DAG entry node not found for {case.name}")
    entry_exec = state.queue_template_node(case.dag.entry_node)
    return QueuedInstance(
        dag=case.dag,
        entry_node=entry_exec.node_id,
        state=state,
        action_results={},
    )


def _queue_benchmark_instances(
    backend: PostgresBackend,
    cases: dict[str, BenchmarkCase],
    count_per_case: int,
    *,
    batch_size: int = 250,
) -> int:
    case_names: list[str] = []
    for name in cases:
        case_names.extend([name] * count_per_case)
    random.shuffle(case_names)

    queued = 0
    batch: list[QueuedInstance] = []
    for name in case_names:
        batch.append(_build_instance(cases[name]))
        if len(batch) >= batch_size:
            backend.queue_instances(batch)
            queued += len(batch)
            batch.clear()
    if batch:
        backend.queue_instances(batch)
        queued += len(batch)
    return queued


def _format_query_counts(counts: dict[str, int]) -> str:
    lines = ["Postgres query counts:"]
    for name in sorted(counts):
        lines.append(f"  {name}: {counts[name]}")
    return "\n".join(lines)


def _median_from_counts(counts: dict[int, int]) -> int:
    total = sum(counts.values())
    if total <= 0:
        return 0
    threshold = (total + 1) // 2
    running = 0
    for size in sorted(counts):
        running += counts[size]
        if running >= threshold:
            return size
    return 0


def _format_batch_size_counts(batch_counts: dict[str, dict[int, int]]) -> str:
    lines = ["Postgres batch size p50:"]
    for name in sorted(batch_counts):
        counts = batch_counts[name]
        if not counts:
            continue
        median = _median_from_counts(counts)
        total = sum(counts.values())
        lines.append(f"  {name}: p50={median} batches={total}")
    return "\n".join(lines)


async def _run_benchmark(
    count_per_case: int, base: int, batch_size: int
) -> tuple[dict[str, int], dict[str, dict[int, int]]]:
    cases = _build_cases(base)
    backend = PostgresBackend()
    backend.clear_all()
    total = _queue_benchmark_instances(backend, cases, count_per_case, batch_size=batch_size)
    print(f"Queued {total} instances across {len(cases)} IR jobs")

    worker_pool = InlineWorkerPool(ACTION_REGISTRY)
    runloop = RunLoop(worker_pool, backend)
    await runloop.run()
    return backend.query_counts(), backend.batch_size_counts()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark mixed IR workloads against the Postgres backend."
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10_000,
        help="Instances per IR job.",
    )
    parser.add_argument(
        "--base",
        type=int,
        default=5,
        help="Base input value for the smoke IR job.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=250,
        help="Batch size for queueing instances.",
    )
    args = parser.parse_args()

    profiler = Profiler()
    profiler.start()
    error: Exception | None = None
    query_counts: dict[str, int] | None = None
    batch_counts: dict[str, dict[int, int]] | None = None
    try:
        query_counts, batch_counts = asyncio.run(
            _run_benchmark(args.count, args.base, args.batch_size)
        )
    except Exception as exc:  # noqa: BLE001 - show profile on failure
        error = exc
    finally:
        profiler.stop()
        print(profiler.output_text(unicode=False, color=False))
        if query_counts:
            print(_format_query_counts(query_counts))
        if batch_counts:
            print(_format_batch_size_counts(batch_counts))
    if error is not None:
        raise error
