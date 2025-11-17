from __future__ import annotations

from dataclasses import dataclass
from typing import List

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
    assert python_block.kwargs["definitions"] == [
        'def helper_threshold(record: "Record") -> bool:\n    return record.amount > 10'
    ]
    summarize_node = next(node for node in dag.nodes if node.action == "summarize")
    assert summarize_node.depends_on == [python_block.id]
