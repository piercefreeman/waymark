from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import List

import pytest

from carabiner_worker.actions import action
from carabiner_worker.workflow import RetryPolicy, Workflow
from carabiner_worker.workflow_dag import (
    RETURN_VARIABLE,
    UNLIMITED_RETRIES,
    WorkflowDag,
    build_workflow_dag,
)


def helper_threshold(record: "Record") -> bool:
    return record.amount > 10


@dataclass
class Record:
    amount: float


@action
async def fetch_records() -> List[Record]:
    raise NotImplementedError


@action
async def summarize(values: List[float]) -> float:
    raise NotImplementedError


@action
async def persist_summary(total: float) -> None:
    raise NotImplementedError


@action
async def fetch_number(idx: int) -> int:
    raise NotImplementedError


@action
async def double_number(value: int) -> int:
    raise NotImplementedError


@action
async def positional_action(prefix: str, count: int) -> str:
    raise NotImplementedError


class SampleWorkflow(Workflow):
    async def run(self) -> float:
        records = await fetch_records()
        positives: List[float] = []
        for record in records:
            if helper_threshold(record):
                positives.append(record.amount)
        total = await summarize(values=positives)
        await persist_summary(total=total)
        return sum(positives)


def test_build_workflow_dag_with_python_block() -> None:
    dag = build_workflow_dag(SampleWorkflow)
    assert isinstance(dag, WorkflowDag)
    actions = [node.action for node in dag.nodes]
    assert actions == [
        "fetch_records",
        "python_block",
        "summarize",
        "persist_summary",
        "python_block",
    ]
    python_block = next(node for node in dag.nodes if node.action == "python_block")
    assert "for record in records" in python_block.kwargs["code"]
    assert (
        python_block.kwargs["definitions"]
        == 'def helper_threshold(record: "Record") -> bool:\n    return record.amount > 10'
    )
    summarize_node = next(node for node in dag.nodes if node.action == "summarize")
    assert summarize_node.depends_on == [python_block.id]
    assert summarize_node.produces == ["total"]
    assert dag.nodes[0].wait_for_sync == []
    assert dag.nodes[1].wait_for_sync == [dag.nodes[0].id]
    assert dag.nodes[2].wait_for_sync == [dag.nodes[1].id]
    assert dag.nodes[0].produces == ["records"]
    assert dag.nodes[-1].produces == [RETURN_VARIABLE]
    assert dag.return_variable == RETURN_VARIABLE
    assert dag.nodes[0].module == __name__

    # Execute the captured python block to ensure it remains valid.
    sample_records = [Record(5), Record(20)]
    namespace: dict[str, object] = {
        "records": sample_records,
        "summary": type(
            "Summary",
            (),
            {"transactions": type("Txns", (), {"records": sample_records})()},
        )(),
        "top_spenders": [],
        "positives": [],
        "helper_threshold": helper_threshold,
    }
    exec(python_block.kwargs["code"], namespace)  # noqa: S102 - intentional
    assert namespace["positives"] == [20]


class ActionAliasWorkflow(Workflow):
    async def run(self) -> None:
        alias = fetch_records
        await alias()


def test_action_reference_assignment_is_rejected() -> None:
    with pytest.raises(ValueError, match="fetch_records"):
        build_workflow_dag(ActionAliasWorkflow)


class ActionCollectionWorkflow(Workflow):
    async def run(self) -> None:
        callbacks = {"summaries": summarize}
        await persist_summary(total=float(len(callbacks)))


def test_action_reference_in_literal_is_rejected() -> None:
    with pytest.raises(ValueError, match="summarize"):
        build_workflow_dag(ActionCollectionWorkflow)


class ListComprehensionWorkflow(Workflow):
    async def run(self) -> None:
        records = await fetch_records()
        await asyncio.gather(*[persist_summary(total=record.amount) for record in records])


def test_list_comprehension_action_reference_allowed() -> None:
    dag = build_workflow_dag(ListComprehensionWorkflow)
    actions = [node.action for node in dag.nodes]
    assert actions[0] == "fetch_records"


class ConditionalWorkflow(Workflow):
    async def run(self) -> None:
        records = await fetch_records()
        if len(records) > 1:
            total = await summarize(values=[record.amount for record in records])
        else:
            total = await summarize(values=[record.amount for record in records[:1]])
        await persist_summary(total=total)


def test_conditional_action_branch_creates_merge_node() -> None:
    dag = build_workflow_dag(ConditionalWorkflow)
    actions = [node.action for node in dag.nodes]
    assert actions == [
        "fetch_records",
        "summarize",
        "summarize",
        "python_block",
        "persist_summary",
        "python_block",
    ]
    true_branch = dag.nodes[1]
    false_branch = dag.nodes[2]
    assert true_branch.guard == "(len(records) > 1)"
    assert false_branch.guard == "not ((len(records) > 1))"
    merge_node = dag.nodes[3]
    assert merge_node.action == "python_block"
    assert merge_node.produces == ["total"]
    assert merge_node.depends_on == sorted([true_branch.id, false_branch.id])
    final_node = dag.nodes[4]
    assert final_node.depends_on == [merge_node.id]
    assert dag.nodes[-1].produces == [RETURN_VARIABLE]


class MissingElseWorkflow(Workflow):
    async def run(self) -> None:
        records = await fetch_records()
        if records:
            await summarize(values=[record.amount for record in records])


def test_conditional_action_requires_else_branch() -> None:
    with pytest.raises(ValueError, match="requires an else branch"):
        build_workflow_dag(MissingElseWorkflow)


class MultiStatementConditionalWorkflow(Workflow):
    async def run(self) -> None:
        records = await fetch_records()
        if records:
            await summarize(values=[record.amount for record in records])
            await persist_summary(total=1.0)
        else:
            await summarize(values=[record.amount for record in records])


def test_conditional_action_rejects_multiple_statements() -> None:
    with pytest.raises(ValueError, match="single action call"):
        build_workflow_dag(MultiStatementConditionalWorkflow)


class GatherWorkflow(Workflow):
    async def run(self) -> None:
        numbers = await asyncio.gather(fetch_number(idx=1), fetch_number(idx=2))
        await summarize(values=list(numbers))


def test_asyncio_gather_assignment_builds_collection_node() -> None:
    dag = build_workflow_dag(GatherWorkflow)
    actions = [node.action for node in dag.nodes]
    assert actions == ["fetch_number", "fetch_number", "python_block", "summarize", "python_block"]
    first, second, collection, summary, _return_block = dag.nodes
    assert first.produces == ["numbers__item0"]
    assert second.produces == ["numbers__item1"]
    assert collection.kwargs["code"] == "numbers = [numbers__item0, numbers__item1]"
    assert collection.produces == ["numbers"]
    assert summary.depends_on == [collection.id]


class GatherAndMapWorkflow(Workflow):
    async def run(self) -> None:
        numbers = await asyncio.gather(fetch_number(idx=1), fetch_number(idx=2))
        doubled = [await double_number(value=value) for value in numbers]
        await summarize(values=doubled)


def test_list_comprehension_of_actions_expands_nodes_per_item() -> None:
    dag = build_workflow_dag(GatherAndMapWorkflow)
    actions = [node.action for node in dag.nodes]
    assert actions == [
        "fetch_number",
        "fetch_number",
        "python_block",
        "double_number",
        "double_number",
        "python_block",
        "summarize",
        "python_block",
    ]
    doubled_collection = next(
        node
        for node in dag.nodes
        if node.action == "python_block"
        and node.kwargs.get("code") == "doubled = [doubled__item0, doubled__item1]"
    )
    assert doubled_collection.kwargs["code"] == "doubled = [doubled__item0, doubled__item1]"
    summarize_node = next(node for node in dag.nodes if node.action == "summarize")
    assert summarize_node.depends_on == [doubled_collection.id]


class PositionalArgsWorkflow(Workflow):
    async def run(self) -> None:
        await positional_action("greeting", 3)


def test_actions_support_positional_arguments() -> None:
    dag = build_workflow_dag(PositionalArgsWorkflow)
    node = dag.nodes[0]
    assert node.action == "positional_action"
    assert node.kwargs == {"prefix": "'greeting'", "count": "3"}


class ReturnValueWorkflow(Workflow):
    async def run(self) -> float:
        total = await summarize(values=[1.0])
        return total


def test_return_variable_tracks_existing_assignment() -> None:
    dag = build_workflow_dag(ReturnValueWorkflow)
    assert dag.return_variable == "total"
    assert dag.nodes[-1].action == "summarize"


class TryExceptWorkflow(Workflow):
    async def run(self) -> None:
        try:
            await fetch_number(idx=1)
            await double_number(value=2)
        except ValueError:
            await persist_summary(total=0.0)


def test_try_except_builds_exception_edges() -> None:
    dag = build_workflow_dag(TryExceptWorkflow)
    assert isinstance(dag, WorkflowDag)
    try_nodes = [node for node in dag.nodes if node.action in {"fetch_number", "double_number"}]
    assert len(try_nodes) == 2
    assert try_nodes[0].guard is None
    assert "__workflow_exceptions" in (try_nodes[1].guard or "")
    handler = next(node for node in dag.nodes if node.action == "persist_summary")
    assert handler.exception_edges
    assert handler.guard is not None and "__workflow_exceptions" in handler.guard


class InvalidReturnWorkflow(Workflow):
    async def run(self) -> float:
        return await summarize(values=[1.0])


def test_return_statement_cannot_await() -> None:
    with pytest.raises(ValueError, match="cannot directly await"):
        build_workflow_dag(InvalidReturnWorkflow)


class RunActionPolicyWorkflow(Workflow):
    async def run(self) -> None:
        await self.run_action(
            summarize(values=[1.0]),
            retry=RetryPolicy(attempts=None),
            timeout=timedelta(minutes=10),
        )


def test_run_action_records_timeout_and_retry_metadata() -> None:
    dag = build_workflow_dag(RunActionPolicyWorkflow)
    node = next(n for n in dag.nodes if n.action == "summarize")
    assert node.timeout_seconds == 600
    assert node.max_retries == UNLIMITED_RETRIES


class LimitedRetryWorkflow(Workflow):
    async def run(self) -> None:
        await self.run_action(fetch_number(idx=42), retry=RetryPolicy(attempts=2))


def test_run_action_limited_retry_metadata() -> None:
    dag = build_workflow_dag(LimitedRetryWorkflow)
    node = next(n for n in dag.nodes if n.action == "fetch_number")
    assert node.max_retries == 2
    assert node.timeout_seconds is None
