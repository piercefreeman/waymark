import asyncio
import math
from dataclasses import dataclass
from datetime import timedelta
from typing import List

import pytest

from rappel.actions import action
from rappel.workflow import ExponentialBackoff, LinearBackoff, RetryPolicy, Workflow
from rappel.workflow_dag import (
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


class ReturnGatherWorkflow(Workflow):
    async def run(self) -> tuple[int, int]:
        return await asyncio.gather(fetch_number(idx=1), fetch_number(idx=2))


def test_return_statement_can_await_gather_actions() -> None:
    dag = build_workflow_dag(ReturnGatherWorkflow)
    assert dag.return_variable == RETURN_VARIABLE
    actions = [node.action for node in dag.nodes]
    assert actions == ["fetch_number", "fetch_number", "python_block"]
    collection = dag.nodes[-1]
    assert collection.produces == [RETURN_VARIABLE]
    assert (
        collection.kwargs["code"]
        == f"{RETURN_VARIABLE} = [{RETURN_VARIABLE}__item0, {RETURN_VARIABLE}__item1]"
    )
    assert collection.depends_on == [dag.nodes[0].id, dag.nodes[1].id]


class ForLoopActionWorkflow(Workflow):
    async def run(self) -> None:
        numbers = await asyncio.gather(fetch_number(idx=1), fetch_number(idx=2))
        results = []
        for number in numbers:
            expanded = number + 1
            doubled = await double_number(value=expanded)
            results.append(doubled)
        await summarize(values=results)


def test_for_loop_builds_loop_controller_node() -> None:
    dag = build_workflow_dag(ForLoopActionWorkflow)
    actions = [node.action for node in dag.nodes]
    assert actions == [
        "fetch_number",
        "fetch_number",
        "python_block",
        "python_block",
        "loop",
        "summarize",
        "python_block",
    ]
    controller = next(node for node in dag.nodes if node.action == "loop")
    assert controller.produces == ["results"]
    assert controller.loop is not None
    assert controller.loop.iterable_expr == "numbers"
    assert controller.loop.loop_var == "number"
    assert controller.loop.body_kwargs == {"value": "expanded"}
    assert controller.loop.accumulator == "results"
    assert "expanded = number + 1" in (controller.loop.preamble or "")
    numbers_node = next(node for node in dag.nodes if node.produces == ["numbers"])
    assert controller.depends_on == [numbers_node.id]
    summarize_node = next(node for node in dag.nodes if node.action == "summarize")
    assert summarize_node.depends_on == [controller.id]


class WorkflowArgsWorkflow(Workflow):
    async def run(self, user_id: str, count: int = 2) -> None:
        await positional_action(prefix=user_id, count=count)


def test_workflow_args_propagate_to_action_kwargs() -> None:
    dag = build_workflow_dag(WorkflowArgsWorkflow)
    node = dag.nodes[0]
    assert node.kwargs == {"prefix": "user_id", "count": "count"}
    assert node.depends_on == []


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


class ReturnAwaitActionWorkflow(Workflow):
    async def run(self) -> float:
        return await summarize(values=[1.0])


def test_return_statement_can_await_action() -> None:
    dag = build_workflow_dag(ReturnAwaitActionWorkflow)
    assert dag.return_variable == RETURN_VARIABLE
    last = dag.nodes[-1]
    assert last.action == "summarize"
    assert last.produces == [RETURN_VARIABLE]


class NoReturnWorkflow(Workflow):
    async def run(self) -> None:
        await fetch_number(idx=1)


def test_missing_return_defaults_to_none() -> None:
    dag = build_workflow_dag(NoReturnWorkflow)
    assert dag.return_variable == RETURN_VARIABLE
    assert dag.nodes[-1].action == "python_block"
    assert dag.nodes[-1].produces == [RETURN_VARIABLE]
    assert dag.nodes[-1].kwargs["code"] == f"{RETURN_VARIABLE} = None"


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


class TypedTryExceptWorkflow(Workflow):
    async def run(self) -> None:
        try:
            await fetch_number(idx=1)
        except ValueError:
            await summarize(values=[1.0])
        except KeyError:
            await persist_summary(total=0.0)


def test_exception_edges_capture_types() -> None:
    dag = build_workflow_dag(TypedTryExceptWorkflow)
    handlers = [n for n in dag.nodes if n.action in {"summarize", "persist_summary"}]
    assert len(handlers) == 2
    for handler in handlers:
        assert handler.exception_edges
    types = {
        (edge.exception_type, edge.exception_module) for h in handlers for edge in h.exception_edges
    }
    assert ("ValueError", None) in types
    assert ("KeyError", None) in types


class InvalidReturnWorkflow(Workflow):
    async def run(self) -> None:
        return await asyncio.sleep(0)


def test_return_statement_cannot_await() -> None:
    with pytest.raises(ValueError, match="only supported for action calls"):
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


class ImportBlockWorkflow(Workflow):
    async def run(self) -> None:
        result = math.sqrt(4)
        await persist_summary(total=result)


def test_python_block_captures_imports() -> None:
    dag = build_workflow_dag(ImportBlockWorkflow)
    block = next(node for node in dag.nodes if node.action == "python_block")
    assert "import math" in block.kwargs["imports"]
    assert "math.sqrt(4)" in block.kwargs["code"]


# Sleep tests


class SleepWorkflow(Workflow):
    async def run(self) -> None:
        await fetch_records()
        await asyncio.sleep(60)
        await persist_summary(total=1.0)


def test_sleep_creates_sleep_node() -> None:
    dag = build_workflow_dag(SleepWorkflow)
    actions = [node.action for node in dag.nodes]
    # python_block at end is for implicit return
    assert actions == ["fetch_records", "sleep", "persist_summary", "python_block"]
    sleep_node = next(n for n in dag.nodes if n.action == "sleep")
    assert sleep_node.sleep_duration_expr == "60"
    # Sleep node should depend on previous node
    assert sleep_node.wait_for_sync == [dag.nodes[0].id]
    # Next action should depend on sleep node
    persist_node = next(n for n in dag.nodes if n.action == "persist_summary")
    assert persist_node.wait_for_sync == [sleep_node.id]


class SleepWithMethodCallWorkflow(Workflow):
    async def run(self) -> None:
        await asyncio.sleep(timedelta(hours=1, minutes=30).total_seconds())


def test_sleep_with_method_call() -> None:
    # Method calls are stored as expressions and evaluated at runtime
    # (will fail at runtime if the expression can't be evaluated)
    dag = build_workflow_dag(SleepWithMethodCallWorkflow)
    sleep_node = next(n for n in dag.nodes if n.action == "sleep")
    assert sleep_node.sleep_duration_expr == "timedelta(hours=1, minutes=30).total_seconds()"


class SleepWithLocalVariableWorkflow(Workflow):
    async def run(self) -> None:
        delay = 60
        await asyncio.sleep(delay)


def test_sleep_with_local_variable() -> None:
    """Local variables are captured as expressions to be evaluated at runtime."""
    dag = build_workflow_dag(SleepWithLocalVariableWorkflow)
    sleep_node = next(n for n in dag.nodes if n.action == "sleep")
    assert sleep_node.sleep_duration_expr == "delay"


class SleepWithNegativeWorkflow(Workflow):
    async def run(self) -> None:
        await asyncio.sleep(-10)


def test_sleep_with_negative_rejected() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        build_workflow_dag(SleepWithNegativeWorkflow)


class SleepWithFloatWorkflow(Workflow):
    async def run(self) -> None:
        await asyncio.sleep(0.5)


def test_sleep_with_float() -> None:
    dag = build_workflow_dag(SleepWithFloatWorkflow)
    sleep_node = next(n for n in dag.nodes if n.action == "sleep")
    assert sleep_node.sleep_duration_expr == "0.5"


class MultipleSleepsWorkflow(Workflow):
    async def run(self) -> None:
        await fetch_records()
        await asyncio.sleep(10)
        await asyncio.sleep(20)
        await persist_summary(total=1.0)


def test_multiple_sleeps() -> None:
    dag = build_workflow_dag(MultipleSleepsWorkflow)
    actions = [node.action for node in dag.nodes]
    # python_block at end is for implicit return
    assert actions == ["fetch_records", "sleep", "sleep", "persist_summary", "python_block"]
    sleep_nodes = [n for n in dag.nodes if n.action == "sleep"]
    assert sleep_nodes[0].sleep_duration_expr == "10"
    assert sleep_nodes[1].sleep_duration_expr == "20"


class SleepWithVariableExprWorkflow(Workflow):
    async def run(self, delay: int) -> None:
        await asyncio.sleep(delay)
        await persist_summary(total=1.0)


def test_sleep_with_variable_expression() -> None:
    """Variables should be resolved at scheduling time."""
    dag = build_workflow_dag(SleepWithVariableExprWorkflow)
    sleep_node = next(n for n in dag.nodes if n.action == "sleep")
    # The expression is stored as-is, will be evaluated at runtime
    assert sleep_node.sleep_duration_expr == "delay"


class SleepWithArithmeticWorkflow(Workflow):
    async def run(self, hours: int) -> None:
        await asyncio.sleep(hours * 60 * 60)
        await persist_summary(total=1.0)


def test_sleep_with_arithmetic_expression() -> None:
    """Arithmetic expressions should be stored and evaluated at runtime."""
    dag = build_workflow_dag(SleepWithArithmeticWorkflow)
    sleep_node = next(n for n in dag.nodes if n.action == "sleep")
    assert sleep_node.sleep_duration_expr == "hours * 60 * 60"


# --- Backoff Policy Tests ---


class LinearBackoffWorkflow(Workflow):
    async def run(self) -> None:
        await self.run_action(
            fetch_number(idx=1),
            retry=RetryPolicy(attempts=3),
            backoff=LinearBackoff(base_delay_ms=500),
        )


def test_linear_backoff_parses_static_literal() -> None:
    """LinearBackoff with static int literal should parse correctly."""
    dag = build_workflow_dag(LinearBackoffWorkflow)
    node = next(n for n in dag.nodes if n.action == "fetch_number")
    assert node.backoff is not None
    assert isinstance(node.backoff, LinearBackoff)
    assert node.backoff.base_delay_ms == 500


class ExponentialBackoffWorkflow(Workflow):
    async def run(self) -> None:
        await self.run_action(
            fetch_number(idx=1),
            retry=RetryPolicy(attempts=5),
            backoff=ExponentialBackoff(base_delay_ms=1000, multiplier=3.0),
        )


def test_exponential_backoff_parses_static_literals() -> None:
    """ExponentialBackoff with static literals should parse correctly."""
    dag = build_workflow_dag(ExponentialBackoffWorkflow)
    node = next(n for n in dag.nodes if n.action == "fetch_number")
    assert node.backoff is not None
    assert isinstance(node.backoff, ExponentialBackoff)
    assert node.backoff.base_delay_ms == 1000
    assert node.backoff.multiplier == 3.0


class LinearBackoffVariableWorkflow(Workflow):
    async def run(self, delay: int) -> None:
        await self.run_action(
            fetch_number(idx=1),
            retry=RetryPolicy(attempts=3),
            backoff=LinearBackoff(base_delay_ms=delay),
        )


def test_linear_backoff_rejects_variable_reference() -> None:
    """LinearBackoff with variable reference should raise ValueError."""
    with pytest.raises(ValueError, match="must be a numeric literal"):
        build_workflow_dag(LinearBackoffVariableWorkflow)


class ExponentialBackoffVariableWorkflow(Workflow):
    async def run(self, mult: float) -> None:
        await self.run_action(
            fetch_number(idx=1),
            retry=RetryPolicy(attempts=3),
            backoff=ExponentialBackoff(base_delay_ms=1000, multiplier=mult),
        )


def test_exponential_backoff_rejects_variable_reference() -> None:
    """ExponentialBackoff with variable reference should raise ValueError."""
    with pytest.raises(ValueError, match="must be a numeric literal"):
        build_workflow_dag(ExponentialBackoffVariableWorkflow)
