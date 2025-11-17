"""Self-contained demo: parse a workflow's run() AST into a DAG."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import List

from carabiner_worker.actions import action
from carabiner_worker.workflow import Workflow
from carabiner_worker.workflow_dag import build_workflow_dag


def _format_currency(value: float) -> str:
    return f"${value:,.2f}"


def _is_high_value(record: "TransactionRecord") -> bool:
    return record.amount > 100


@dataclass
class Profile:
    user_id: str
    status: str


@dataclass
class Transactions:
    total: float
    count: int
    records: List["TransactionRecord"]


@dataclass
class Summary:
    lifetime_value: float
    transactions: Transactions


@dataclass
class EnrichedSummary:
    summary: Summary
    tier: str


@dataclass
class TransactionRecord:
    amount: float
    currency: str = "USD"


@action
async def fetch_profile(user_id: str) -> Profile:
    raise NotImplementedError


@action
async def load_transactions(user_id: str) -> Transactions:
    raise NotImplementedError


@action
async def compute_summary(profile: Profile, txns: Transactions) -> Summary:
    raise NotImplementedError


@action
async def enrich_summary(summary: Summary) -> EnrichedSummary:
    raise NotImplementedError


@action
async def persist_summary(
    user_id: str, summary: EnrichedSummary, pretty: str, total_spent: float
) -> None:
    raise NotImplementedError


@action
async def record_high_value(user_id: str, top_spenders: List[float]) -> None:
    raise NotImplementedError


class CustomerSummaryWorkflow(Workflow):
    def __init__(self, user_id: str):
        self.user_id = user_id

    async def run(self) -> EnrichedSummary:
        profile, txns = await asyncio.gather(
            fetch_profile(user_id=self.user_id),
            load_transactions(user_id=self.user_id),
        )
        summary = await compute_summary(
            profile=profile,
            txns=txns,
        )
        enriched = await enrich_summary(summary=summary)
        pretty = _format_currency(summary.transactions.total)
        top_spenders: List[float] = []
        for record in summary.transactions.records:
            if _is_high_value(record):
                top_spenders.append(record.amount)
        await persist_summary(
            user_id=self.user_id,
            summary=enriched,
            pretty=pretty,
            total_spent=summary.transactions.total,
        )
        await record_high_value(user_id=self.user_id, top_spenders=top_spenders)
        return enriched


if __name__ == "__main__":
    dag = build_workflow_dag(CustomerSummaryWorkflow)
    print("Workflow DAG:")
    for node in dag.nodes:
        print(
            f"- {node.id}: action={node.action} kwargs={node.kwargs} depends_on={node.depends_on}"
        )
