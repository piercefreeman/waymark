from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import List

import pytest

from carabiner_worker.actions import action
from carabiner_worker.workflow import Workflow
from carabiner_worker.workflow_dag import WorkflowDag, build_workflow_dag


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
    assert dag.nodes[-1].produces == []
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
    assert actions == ["fetch_records", "summarize", "summarize", "python_block", "persist_summary"]
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
