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
    assert false_branch.guard == "not (len(records) > 1)"
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


def test_conditional_action_rejects_multiple_actions_per_branch() -> None:
    """Branches with multiple action calls should be rejected."""
    with pytest.raises(ValueError, match="exactly one action call per branch"):
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


# --- Module Reference Tests for Return Statements ---


class ResultContainer:
    """A user-defined class to test module reference resolution in return statements."""

    def __init__(self, value: float, label: str) -> None:
        self.value = value
        self.label = label


class ReturnUserDefinedClassWorkflow(Workflow):
    async def run(self) -> ResultContainer:
        total = await summarize(values=[1.0, 2.0, 3.0])
        return ResultContainer(value=total, label="computed")


def test_return_statement_resolves_user_defined_class() -> None:
    """Return statements using user-defined classes should include definitions."""
    dag = build_workflow_dag(ReturnUserDefinedClassWorkflow)
    return_block = dag.nodes[-1]
    assert return_block.action == "python_block"
    assert RETURN_VARIABLE in return_block.produces
    # The definitions should include the ResultContainer class
    assert "class ResultContainer" in return_block.kwargs["definitions"]
    # Verify the class definition can be executed
    assert "__init__" in return_block.kwargs["definitions"]


class BaseClass:
    """Base class for testing transitive dependency resolution."""

    base_field: str = "default"


class DerivedClass(BaseClass):
    """Derived class that inherits from BaseClass."""

    def __init__(self, value: float) -> None:
        self.value = value


class ReturnDerivedClassWorkflow(Workflow):
    async def run(self) -> DerivedClass:
        total = await summarize(values=[1.0])
        return DerivedClass(value=total)


def test_return_statement_resolves_base_class_dependencies() -> None:
    """Return statements using derived classes should include base class definitions."""
    dag = build_workflow_dag(ReturnDerivedClassWorkflow)
    return_block = dag.nodes[-1]
    assert return_block.action == "python_block"
    # The definitions should include both DerivedClass and its base class
    definitions = return_block.kwargs["definitions"]
    assert "class DerivedClass" in definitions
    assert "class BaseClass" in definitions


class ReturnExpressionWithImportWorkflow(Workflow):
    async def run(self) -> float:
        total = await summarize(values=[1.0, 2.0])
        return math.sqrt(total)


def test_return_expression_resolves_imports() -> None:
    """Return expressions using imported modules should include imports."""
    dag = build_workflow_dag(ReturnExpressionWithImportWorkflow)
    return_block = dag.nodes[-1]
    assert return_block.action == "python_block"
    assert "import math" in return_block.kwargs["imports"]


# --- Multi-Statement Conditional Tests ---


class MultiStatementConditionalWithPostambleWorkflow(Workflow):
    async def run(self) -> float:
        records = await fetch_records()
        if len(records) > 5:
            result = await summarize(values=[r.amount for r in records])
            multiplier = 2.0
        else:
            result = await summarize(values=[r.amount for r in records[:5]])
            multiplier = 1.0
        return result * multiplier


def test_multi_statement_conditional_with_postamble() -> None:
    """Conditional branches with statements after the action should work."""
    dag = build_workflow_dag(MultiStatementConditionalWithPostambleWorkflow)
    actions = [node.action for node in dag.nodes]
    # Should have: fetch_records, 2x summarize (guarded), merge (python_block), return (python_block)
    assert "fetch_records" in actions
    assert actions.count("summarize") == 2
    # Find merge node - should produce both 'result' and 'multiplier'
    merge_candidates = [
        n for n in dag.nodes if n.action == "python_block" and "result" in n.produces
    ]
    assert len(merge_candidates) >= 1
    merge_node = merge_candidates[0]
    assert "multiplier" in merge_node.produces


class MultiStatementConditionalWithPreambleWorkflow(Workflow):
    async def run(self) -> float:
        records = await fetch_records()
        if len(records) > 5:
            adjusted = [r.amount * 2 for r in records]
            result = await summarize(values=adjusted)
        else:
            adjusted = [r.amount for r in records]
            result = await summarize(values=adjusted)
        return result


def test_multi_statement_conditional_with_preamble() -> None:
    """Conditional branches with statements before the action should work."""
    dag = build_workflow_dag(MultiStatementConditionalWithPreambleWorkflow)
    actions = [node.action for node in dag.nodes]
    assert "fetch_records" in actions
    assert actions.count("summarize") == 2
    # Should have preamble python blocks with guards
    preamble_blocks = [
        n
        for n in dag.nodes
        if n.action == "python_block" and "adjusted" in n.produces and n.guard is not None
    ]
    assert len(preamble_blocks) == 2


class ElifChainWorkflow(Workflow):
    async def run(self) -> float:
        value = await fetch_number(idx=1)
        if value >= 100:
            result = await summarize(values=[float(value)])
            _tier = "high"  # noqa: F841
        elif value >= 50:
            result = await summarize(values=[float(value) / 2])
            _tier = "medium"  # noqa: F841
        else:
            result = await summarize(values=[float(value) / 4])
            _tier = "low"  # noqa: F841
        return result


def test_elif_chain_creates_multiple_guarded_branches() -> None:
    """elif chains should create properly guarded action nodes."""
    dag = build_workflow_dag(ElifChainWorkflow)
    actions = [node.action for node in dag.nodes]
    assert "fetch_number" in actions
    # Should have 3 summarize nodes, one for each branch
    assert actions.count("summarize") == 3
    summarize_nodes = [n for n in dag.nodes if n.action == "summarize"]
    # Each should have a guard
    guards: list[str] = [n.guard for n in summarize_nodes if n.guard is not None]
    assert len(guards) == 3
    # The guards should be mutually exclusive
    assert any("value >= 100" in g for g in guards)
    assert any("value >= 50" in g for g in guards)


class InterleavedStatementsWorkflow(Workflow):
    async def run(self) -> list[float]:
        results = []
        value1 = await summarize(values=[1.0])
        results.append(value1)
        value2 = await summarize(values=[2.0])
        results.append(value2)
        return results


def test_interleaved_statements_are_captured() -> None:
    """Statements between actions (like list.append) should be captured."""
    dag = build_workflow_dag(InterleavedStatementsWorkflow)
    actions = [node.action for node in dag.nodes]
    # Should have: init block, action, append block, action, append block, return block
    assert actions.count("summarize") == 2
    assert actions.count("python_block") >= 3  # init + 2 appends + return
    # Find the append blocks
    append_blocks = [
        n for n in dag.nodes if n.action == "python_block" and "append" in n.kwargs.get("code", "")
    ]
    assert len(append_blocks) == 2
    # Each append should depend on a summarize node
    summarize_ids = {n.id for n in dag.nodes if n.action == "summarize"}
    for block in append_blocks:
        assert any(dep in summarize_ids for dep in block.depends_on)


# =====================
# Multi-Action Loop Tests
# =====================


@action
async def validate_order(order: dict) -> dict:
    raise NotImplementedError


@action
async def process_payment(validated: dict) -> dict:
    raise NotImplementedError


@action
async def send_confirmation(payment: dict) -> dict:
    raise NotImplementedError


class MultiActionLoopWorkflow(Workflow):
    """Workflow with multiple actions per loop iteration."""

    async def run(self, orders: list) -> list:
        results = []
        for order in orders:
            validated = await validate_order(order=order)
            payment = await process_payment(validated=validated)
            confirmation = await send_confirmation(payment=payment)
            results.append(confirmation)
        return results


def test_multi_action_loop_builds_body_graph() -> None:
    """Multi-action loops should create a LoopBodyGraph with multiple nodes."""
    dag = build_workflow_dag(MultiActionLoopWorkflow)
    actions = [node.action for node in dag.nodes]

    # Should have a multi_action_loop node
    assert "multi_action_loop" in actions

    controller = next(node for node in dag.nodes if node.action == "multi_action_loop")
    assert controller.produces == ["results"]
    assert controller.multi_action_loop is not None

    loop_spec = controller.multi_action_loop
    assert loop_spec.iterable_expr == "orders"
    assert loop_spec.loop_var == "order"
    assert loop_spec.accumulator == "results"

    # Check body graph
    body_graph = loop_spec.body_graph
    assert body_graph is not None
    assert len(body_graph.nodes) == 3
    assert body_graph.result_variable == "confirmation"

    # Check phase nodes
    phase_ids = [node.id for node in body_graph.nodes]
    assert phase_ids == ["phase_0", "phase_1", "phase_2"]

    # Check actions
    phase_actions = [node.action for node in body_graph.nodes]
    assert phase_actions == ["validate_order", "process_payment", "send_confirmation"]

    # Check dependencies
    phase_0 = body_graph.nodes[0]
    assert phase_0.depends_on == []
    assert phase_0.output_var == "validated"

    phase_1 = body_graph.nodes[1]
    assert phase_1.depends_on == ["phase_0"]
    assert phase_1.output_var == "payment"

    phase_2 = body_graph.nodes[2]
    assert phase_2.depends_on == ["phase_1"]
    assert phase_2.output_var == "confirmation"


class MultiActionLoopWithPreambleWorkflow(Workflow):
    """Workflow with preamble before first action in loop."""

    async def run(self, orders: list) -> list:
        results = []
        for order in orders:
            _order_id = order["id"]  # noqa: F841 - testing preamble capture
            validated = await validate_order(order=order)
            payment = await process_payment(validated=validated)
            results.append(payment)
        return results


def test_multi_action_loop_with_preamble() -> None:
    """Multi-action loops should support preamble statements."""
    dag = build_workflow_dag(MultiActionLoopWithPreambleWorkflow)

    controller = next(node for node in dag.nodes if node.action == "multi_action_loop")
    assert controller.multi_action_loop is not None

    loop_spec = controller.multi_action_loop
    # Preamble should contain the order_id assignment
    assert loop_spec.preamble is not None
    assert "order_id" in loop_spec.preamble

    # Body graph should have 2 nodes
    assert len(loop_spec.body_graph.nodes) == 2


class SingleActionLoopWorkflow(Workflow):
    """Single action loop should still use legacy LoopSpec."""

    async def run(self, numbers: list) -> list:
        results = []
        for number in numbers:
            doubled = await double_number(value=number)
            results.append(doubled)
        return results


def test_single_action_loop_uses_legacy_loop_spec() -> None:
    """Single action loops should use legacy LoopSpec, not multi-action."""
    dag = build_workflow_dag(SingleActionLoopWorkflow)
    actions = [node.action for node in dag.nodes]

    # Should use "loop", not "multi_action_loop"
    assert "loop" in actions
    assert "multi_action_loop" not in actions

    controller = next(node for node in dag.nodes if node.action == "loop")
    assert controller.loop is not None
    assert controller.multi_action_loop is None
