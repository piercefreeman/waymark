"""
Comprehensive tests for IRParser - testing valid workflow patterns.

These tests verify that the IRParser correctly transforms Python AST into
Rappel IR protobuf messages for all supported workflow patterns.
"""

import ast
import textwrap

import pytest

from proto import ir_pb2
from rappel.ir import ActionDefinition, IRParser, IRSerializer, ModuleIndex

# =============================================================================
# Test Fixtures - Example Workflows
# =============================================================================

EXAMPLE_MODULE = """
import asyncio
import math
from datetime import timedelta
from dataclasses import dataclass

# Fake action definitions (in real code these would be @action decorated)
async def fetch_left() -> int: ...
async def fetch_right() -> int: ...
async def double(value: int) -> int: ...
async def summarize(values: list) -> float: ...
async def persist_summary(total: float) -> None: ...
async def fetch_number(idx: int) -> int: ...
async def validate_order(order: dict) -> dict: ...
async def process_payment(validated: dict) -> dict: ...
async def send_confirmation(payment: dict) -> str: ...
async def evaluate_high(value: int) -> str: ...
async def evaluate_medium(value: int) -> str: ...
async def evaluate_low(value: int) -> str: ...
async def risky_action() -> int: ...
async def fallback_action() -> int: ...
async def get_timestamp() -> str: ...
async def positional_action(prefix: str, count: int, suffix: str = "") -> str: ...


@dataclass
class Record:
    amount: float


def helper_threshold(record: Record) -> bool:
    return record.amount > 10


class Workflow:
    pass


class RetryPolicy:
    def __init__(self, attempts=None): pass

class LinearBackoff:
    def __init__(self, base_delay_ms=0): pass

class ExponentialBackoff:
    def __init__(self, base_delay_ms=0, multiplier=2.0): pass
"""


def build_action_defs() -> dict[str, ActionDefinition]:
    """Build action definitions from the example module."""
    tree = ast.parse(EXAMPLE_MODULE)
    action_defs: dict[str, ActionDefinition] = {}
    for node in tree.body:
        if isinstance(node, ast.AsyncFunctionDef):
            param_names = [arg.arg for arg in node.args.args if arg.arg != "self"]
            action_defs[node.name] = ActionDefinition(
                name=node.name,
                module="example_module",
                param_names=param_names,
            )
    return action_defs


@pytest.fixture
def action_defs() -> dict[str, ActionDefinition]:
    return build_action_defs()


@pytest.fixture
def module_index() -> ModuleIndex:
    return ModuleIndex(EXAMPLE_MODULE)


def parse_workflow_class(
    code: str, action_defs: dict[str, ActionDefinition], module_index: ModuleIndex | None = None
) -> ir_pb2.Workflow:
    """Parse a workflow class and return the IR."""
    full_code = EXAMPLE_MODULE + "\n" + textwrap.dedent(code)
    tree = ast.parse(full_code)

    # Find the last class definition (our test workflow)
    workflow_class = None
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name.endswith("Workflow"):
            workflow_class = node

    assert workflow_class is not None, "Could not find workflow class"

    # Find run method
    run_method = None
    for item in workflow_class.body:
        if isinstance(item, ast.AsyncFunctionDef) and item.name == "run":
            run_method = item
            break

    assert run_method is not None, "Could not find run method"

    parser = IRParser(action_defs=action_defs, module_index=module_index, source=full_code)
    workflow = parser.parse_workflow(run_method)
    workflow.name = workflow_class.name
    return workflow


# =============================================================================
# Basic Workflow Pattern Tests
# =============================================================================


class TestSequentialActions:
    """Test sequential action execution patterns."""

    def test_simple_sequential(self, action_defs: dict[str, ActionDefinition]):
        """Test simple sequential action calls."""
        code = """
class SimpleSequentialWorkflow(Workflow):
    async def run(self) -> float:
        records = await fetch_left()
        total = await summarize(values=[records])
        await persist_summary(total=total)
        return total
"""
        workflow = parse_workflow_class(code, action_defs)

        assert workflow.name == "SimpleSequentialWorkflow"
        assert len(workflow.body) == 4  # 3 actions + 1 return

        # First action
        assert workflow.body[0].WhichOneof("kind") == "action_call"
        assert workflow.body[0].action_call.action == "fetch_left"
        assert workflow.body[0].action_call.target == "records"

        # Second action
        assert workflow.body[1].WhichOneof("kind") == "action_call"
        assert workflow.body[1].action_call.action == "summarize"
        assert workflow.body[1].action_call.target == "total"

        # Third action (no target)
        assert workflow.body[2].WhichOneof("kind") == "action_call"
        assert workflow.body[2].action_call.action == "persist_summary"

        # Return
        assert workflow.body[3].WhichOneof("kind") == "return_stmt"
        assert workflow.body[3].return_stmt.expr == "total"

    def test_return_action(self, action_defs: dict[str, ActionDefinition]):
        """Test returning the result of an action directly."""
        code = """
class ReturnActionWorkflow(Workflow):
    async def run(self) -> int:
        return await fetch_left()
"""
        workflow = parse_workflow_class(code, action_defs)

        assert len(workflow.body) == 1
        ret = workflow.body[0].return_stmt
        assert ret.WhichOneof("value") == "action"
        assert ret.action.action == "fetch_left"


class TestGather:
    """Test parallel gather patterns."""

    def test_simple_gather(self, action_defs: dict[str, ActionDefinition]):
        """Test basic asyncio.gather with multiple actions."""
        code = """
class SimpleGatherWorkflow(Workflow):
    async def run(self) -> tuple:
        results = await asyncio.gather(fetch_left(), fetch_right())
        return results
"""
        workflow = parse_workflow_class(code, action_defs)

        assert len(workflow.body) == 2  # gather + return

        gather = workflow.body[0].gather
        assert gather.target == "results"
        assert len(gather.calls) == 2
        assert gather.calls[0].action == "fetch_left"
        assert gather.calls[1].action == "fetch_right"

    def test_return_gather(self, action_defs: dict[str, ActionDefinition]):
        """Test returning gather result directly."""
        code = """
class ReturnGatherWorkflow(Workflow):
    async def run(self) -> tuple:
        return await asyncio.gather(fetch_left(), fetch_right())
"""
        workflow = parse_workflow_class(code, action_defs)

        assert len(workflow.body) == 1
        ret = workflow.body[0].return_stmt
        assert ret.WhichOneof("value") == "gather"
        assert len(ret.gather.calls) == 2

    def test_gather_with_params(self, action_defs: dict[str, ActionDefinition]):
        """Test gather with action parameters."""
        code = """
class GatherWithParamsWorkflow(Workflow):
    async def run(self) -> tuple:
        results = await asyncio.gather(
            fetch_number(idx=1),
            fetch_number(idx=2),
            fetch_number(idx=3)
        )
        return results
"""
        workflow = parse_workflow_class(code, action_defs)

        gather = workflow.body[0].gather
        assert len(gather.calls) == 3
        assert gather.calls[0].kwargs["idx"] == "1"
        assert gather.calls[1].kwargs["idx"] == "2"
        assert gather.calls[2].kwargs["idx"] == "3"


class TestLoops:
    """Test for loop patterns."""

    def test_single_action_loop(self, action_defs: dict[str, ActionDefinition]):
        """Test loop with a single action."""
        code = """
class SingleActionLoopWorkflow(Workflow):
    async def run(self, numbers: list) -> list:
        results = []
        for number in numbers:
            doubled = await double(value=number)
            results.append(doubled)
        return results
"""
        workflow = parse_workflow_class(code, action_defs)

        # Find loop statement
        loop = None
        for stmt in workflow.body:
            if stmt.WhichOneof("kind") == "loop":
                loop = stmt.loop
                break

        assert loop is not None
        assert loop.loop_var == "number"
        assert loop.iterator_expr == "numbers"
        assert "results" in loop.accumulators

        assert len(loop.body) == 1
        assert loop.body[0].action == "double"
        assert loop.body[0].target == "doubled"

        assert len(loop.yields) == 1
        assert loop.yields[0].source_expr == "doubled"
        assert loop.yields[0].accumulator == "results"

    def test_multi_action_loop(self, action_defs: dict[str, ActionDefinition]):
        """Test loop with multiple sequential actions."""
        code = """
class MultiActionLoopWorkflow(Workflow):
    async def run(self, orders: list) -> list:
        confirmations = []
        for order in orders:
            validated = await validate_order(order=order)
            payment = await process_payment(validated=validated)
            confirmation = await send_confirmation(payment=payment)
            confirmations.append(confirmation)
        return confirmations
"""
        workflow = parse_workflow_class(code, action_defs)

        loop = None
        for stmt in workflow.body:
            if stmt.WhichOneof("kind") == "loop":
                loop = stmt.loop
                break

        assert loop is not None
        assert len(loop.body) == 3
        assert loop.body[0].action == "validate_order"
        assert loop.body[1].action == "process_payment"
        assert loop.body[2].action == "send_confirmation"

    def test_loop_with_preamble(self, action_defs: dict[str, ActionDefinition]):
        """Test loop with Python code before the first action."""
        code = """
class LoopWithPreambleWorkflow(Workflow):
    async def run(self, items: list) -> list:
        results = []
        for item in items:
            adjusted = item * 2
            processed = await double(value=adjusted)
            results.append(processed)
        return results
"""
        workflow = parse_workflow_class(code, action_defs)

        loop = None
        for stmt in workflow.body:
            if stmt.WhichOneof("kind") == "loop":
                loop = stmt.loop
                break

        assert loop is not None
        assert len(loop.preamble) == 1
        assert "adjusted = item * 2" in loop.preamble[0].code
        assert "item" in loop.preamble[0].inputs
        assert "adjusted" in loop.preamble[0].outputs

    def test_multi_accumulator_loop(self, action_defs: dict[str, ActionDefinition]):
        """Test loop with multiple accumulators."""
        code = """
class MultiAccumulatorWorkflow(Workflow):
    async def run(self, items: list) -> tuple:
        results = []
        metrics = []
        for item in items:
            processed = await double(value=item)
            results.append(processed)
            metrics.append(processed * 2)
        return (results, metrics)
"""
        workflow = parse_workflow_class(code, action_defs)

        loop = None
        for stmt in workflow.body:
            if stmt.WhichOneof("kind") == "loop":
                loop = stmt.loop
                break

        assert loop is not None
        assert "results" in loop.accumulators
        assert "metrics" in loop.accumulators
        assert len(loop.yields) == 2


class TestConditionals:
    """Test conditional branching patterns."""

    def test_simple_if_else(self, action_defs: dict[str, ActionDefinition]):
        """Test simple if/else with actions."""
        code = """
class SimpleConditionalWorkflow(Workflow):
    async def run(self, value: int) -> str:
        if value > 50:
            result = await evaluate_high(value=value)
        else:
            result = await evaluate_low(value=value)
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        cond = None
        for stmt in workflow.body:
            if stmt.WhichOneof("kind") == "conditional":
                cond = stmt.conditional
                break

        assert cond is not None
        assert len(cond.branches) == 2
        assert cond.target == "result"

        assert "(value > 50)" in cond.branches[0].guard
        assert "not " in cond.branches[1].guard

    def test_if_elif_else(self, action_defs: dict[str, ActionDefinition]):
        """Test if/elif/else chain."""
        code = """
class ElifWorkflow(Workflow):
    async def run(self, value: int) -> str:
        if value >= 75:
            result = await evaluate_high(value=value)
        elif value >= 25:
            result = await evaluate_medium(value=value)
        else:
            result = await evaluate_low(value=value)
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        cond = None
        for stmt in workflow.body:
            if stmt.WhichOneof("kind") == "conditional":
                cond = stmt.conditional
                break

        assert cond is not None
        assert len(cond.branches) == 3

    def test_conditional_with_preamble_postamble(self, action_defs: dict[str, ActionDefinition]):
        """Test conditional branches with preamble and postamble code."""
        code = """
class ConditionalWithExtrasWorkflow(Workflow):
    async def run(self, value: int) -> str:
        if value > 50:
            adjusted = value * 2
            result = await evaluate_high(value=adjusted)
            label = "high"
        else:
            adjusted = value
            result = await evaluate_low(value=adjusted)
            label = "low"
        return f"{label}:{result}"
"""
        workflow = parse_workflow_class(code, action_defs)

        cond = None
        for stmt in workflow.body:
            if stmt.WhichOneof("kind") == "conditional":
                cond = stmt.conditional
                break

        assert cond is not None

        # First branch should have preamble and postamble
        branch1 = cond.branches[0]
        assert len(branch1.preamble) == 1
        assert "adjusted" in branch1.preamble[0].outputs
        assert len(branch1.postamble) == 1
        assert "label" in branch1.postamble[0].outputs

    def test_multi_action_conditional(self, action_defs: dict[str, ActionDefinition]):
        """Test conditional with multiple actions per branch."""
        code = """
class MultiActionConditionalWorkflow(Workflow):
    async def run(self, mode: str) -> float:
        if mode == "full":
            a = await fetch_left()
            b = await double(value=a)
            result = await summarize(values=[b])
        else:
            result = await fetch_right()
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        cond = None
        for stmt in workflow.body:
            if stmt.WhichOneof("kind") == "conditional":
                cond = stmt.conditional
                break

        assert cond is not None

        # First branch has 3 actions
        assert len(cond.branches[0].actions) == 3
        assert cond.branches[0].actions[0].action == "fetch_left"
        assert cond.branches[0].actions[1].action == "double"
        assert cond.branches[0].actions[2].action == "summarize"

        # Second branch has 1 action
        assert len(cond.branches[1].actions) == 1


class TestTryExcept:
    """Test try/except patterns."""

    def test_simple_try_except(self, action_defs: dict[str, ActionDefinition]):
        """Test basic try/except with actions."""
        code = """
class SimpleTryExceptWorkflow(Workflow):
    async def run(self) -> int:
        try:
            result = await risky_action()
        except ValueError:
            result = await fallback_action()
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        te = None
        for stmt in workflow.body:
            if stmt.WhichOneof("kind") == "try_except":
                te = stmt.try_except
                break

        assert te is not None
        assert len(te.try_body) == 1
        assert te.try_body[0].action == "risky_action"

        assert len(te.handlers) == 1
        assert te.handlers[0].exception_types[0].name == "ValueError"
        assert len(te.handlers[0].body) == 1
        assert te.handlers[0].body[0].action == "fallback_action"

    def test_multi_exception_types(self, action_defs: dict[str, ActionDefinition]):
        """Test try/except with multiple exception types."""
        code = """
class MultiExceptWorkflow(Workflow):
    async def run(self) -> int:
        try:
            result = await risky_action()
        except (ValueError, TypeError):
            result = await fallback_action()
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        te = None
        for stmt in workflow.body:
            if stmt.WhichOneof("kind") == "try_except":
                te = stmt.try_except
                break

        assert te is not None
        handler = te.handlers[0]
        assert len(handler.exception_types) == 2
        exc_names = {et.name for et in handler.exception_types}
        assert exc_names == {"ValueError", "TypeError"}

    def test_multi_action_try(self, action_defs: dict[str, ActionDefinition]):
        """Test try block with multiple actions."""
        code = """
class MultiActionTryWorkflow(Workflow):
    async def run(self) -> int:
        try:
            a = await fetch_number(idx=1)
            b = await double(value=a)
            c = await double(value=b)
        except ValueError:
            c = await fallback_action()
        return c
"""
        workflow = parse_workflow_class(code, action_defs)

        te = None
        for stmt in workflow.body:
            if stmt.WhichOneof("kind") == "try_except":
                te = stmt.try_except
                break

        assert te is not None
        assert len(te.try_body) == 3
        assert te.try_body[0].action == "fetch_number"
        assert te.try_body[1].action == "double"
        assert te.try_body[2].action == "double"


class TestSleep:
    """Test sleep patterns."""

    def test_simple_sleep(self, action_defs: dict[str, ActionDefinition]):
        """Test asyncio.sleep call."""
        code = """
class SleepWorkflow(Workflow):
    async def run(self) -> str:
        started = await get_timestamp()
        await asyncio.sleep(60)
        ended = await get_timestamp()
        return f"{started}-{ended}"
"""
        workflow = parse_workflow_class(code, action_defs)

        sleep = None
        for stmt in workflow.body:
            if stmt.WhichOneof("kind") == "sleep":
                sleep = stmt.sleep
                break

        assert sleep is not None
        assert sleep.duration_expr == "60"

    def test_sleep_with_expression(self, action_defs: dict[str, ActionDefinition]):
        """Test sleep with expression for duration."""
        code = """
class SleepExprWorkflow(Workflow):
    async def run(self, wait_time: int) -> str:
        await asyncio.sleep(wait_time * 2)
        result = await get_timestamp()
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        sleep = None
        for stmt in workflow.body:
            if stmt.WhichOneof("kind") == "sleep":
                sleep = stmt.sleep
                break

        assert sleep is not None
        assert sleep.duration_expr == "wait_time * 2"


class TestSpread:
    """Test spread (list comprehension) patterns."""

    def test_simple_spread(self, action_defs: dict[str, ActionDefinition]):
        """Test list comprehension with action."""
        code = """
class SpreadWorkflow(Workflow):
    async def run(self) -> list:
        numbers = await asyncio.gather(fetch_number(idx=1), fetch_number(idx=2))
        doubled = [await double(value=n) for n in numbers]
        return doubled
"""
        workflow = parse_workflow_class(code, action_defs)

        spread = None
        for stmt in workflow.body:
            if stmt.WhichOneof("kind") == "spread":
                spread = stmt.spread
                break

        assert spread is not None
        assert spread.target == "doubled"
        assert spread.iterable == "numbers"
        assert spread.loop_var == "n"
        assert spread.action.action == "double"


class TestRunAction:
    """Test run_action with policies."""

    def test_run_action_with_timeout(self, action_defs: dict[str, ActionDefinition]):
        """Test run_action with timeout policy."""
        code = """
class TimeoutWorkflow(Workflow):
    async def run(self) -> int:
        result = await self.run_action(risky_action(), timeout=300)
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        action = workflow.body[0].action_call
        assert action.action == "risky_action"
        assert action.config.timeout_seconds == 300

    def test_run_action_with_retry(self, action_defs: dict[str, ActionDefinition]):
        """Test run_action with retry policy."""
        code = """
class RetryWorkflow(Workflow):
    async def run(self) -> int:
        result = await self.run_action(
            risky_action(),
            retry=RetryPolicy(attempts=3)
        )
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        action = workflow.body[0].action_call
        assert action.config.max_retries == 3

    def test_run_action_with_linear_backoff(self, action_defs: dict[str, ActionDefinition]):
        """Test run_action with linear backoff."""
        code = """
class LinearBackoffWorkflow(Workflow):
    async def run(self) -> int:
        result = await self.run_action(
            risky_action(),
            backoff=LinearBackoff(base_delay_ms=1000)
        )
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        action = workflow.body[0].action_call
        assert action.config.backoff.kind == ir_pb2.BackoffConfig.KIND_LINEAR
        assert action.config.backoff.base_delay_ms == 1000

    def test_run_action_with_exponential_backoff(self, action_defs: dict[str, ActionDefinition]):
        """Test run_action with exponential backoff."""
        code = """
class ExponentialBackoffWorkflow(Workflow):
    async def run(self) -> int:
        result = await self.run_action(
            risky_action(),
            backoff=ExponentialBackoff(base_delay_ms=500, multiplier=3.0)
        )
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        action = workflow.body[0].action_call
        assert action.config.backoff.kind == ir_pb2.BackoffConfig.KIND_EXPONENTIAL
        assert action.config.backoff.base_delay_ms == 500
        assert action.config.backoff.multiplier == 3.0


class TestPositionalArgs:
    """Test positional argument mapping."""

    def test_positional_args(self, action_defs: dict[str, ActionDefinition]):
        """Test that positional args are mapped to kwargs using signature."""
        code = """
class PositionalArgsWorkflow(Workflow):
    async def run(self) -> str:
        result = await positional_action("hello", 42, suffix="!")
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        action = workflow.body[0].action_call
        assert action.action == "positional_action"
        assert action.kwargs["prefix"] == "'hello'"
        assert action.kwargs["count"] == "42"
        assert action.kwargs["suffix"] == "'!'"


class TestPythonBlocks:
    """Test Python block patterns."""

    def test_python_block_io(
        self, action_defs: dict[str, ActionDefinition], module_index: ModuleIndex
    ):
        """Test input/output analysis for Python blocks."""
        code = """
class PythonBlockWorkflow(Workflow):
    async def run(self) -> list:
        raw = await asyncio.gather(fetch_left(), fetch_right())
        combined = list(raw) + [100]
        doubled = [await double(value=x) for x in combined]
        return doubled
"""
        workflow = parse_workflow_class(code, action_defs, module_index)

        # Find python block
        py_block = None
        for stmt in workflow.body:
            if stmt.WhichOneof("kind") == "python_block":
                py_block = stmt.python_block
                break

        assert py_block is not None
        assert "combined" in py_block.outputs
        assert "raw" in py_block.inputs

    def test_python_block_with_math_import(
        self, action_defs: dict[str, ActionDefinition], module_index: ModuleIndex
    ):
        """Test Python block that uses imported module."""
        code = """
class MathWorkflow(Workflow):
    async def run(self) -> str:
        raw = await asyncio.gather(fetch_left(), fetch_right())
        combined = int(math.sqrt(sum(raw)))
        if combined > 10:
            result = await evaluate_high(value=combined)
        else:
            result = await evaluate_low(value=combined)
        return result
"""
        workflow = parse_workflow_class(code, action_defs, module_index)

        # Find python block (first one after gather)
        py_block = None
        for stmt in workflow.body:
            if stmt.WhichOneof("kind") == "python_block":
                py_block = stmt.python_block
                break

        assert py_block is not None
        # Should have math import captured
        assert any("math" in imp for imp in py_block.imports)


class TestWorkflowParams:
    """Test workflow parameter handling."""

    def test_workflow_with_params(self, action_defs: dict[str, ActionDefinition]):
        """Test workflow with typed parameters."""
        code = """
class ParamsWorkflow(Workflow):
    async def run(self, items: list[int], threshold: float = 0.5) -> list[str]:
        results = []
        for item in items:
            doubled = await double(value=item)
            results.append(doubled)
        return results
"""
        workflow = parse_workflow_class(code, action_defs)

        assert len(workflow.params) == 2
        assert workflow.params[0].name == "items"
        assert workflow.params[0].type_annotation == "list[int]"
        assert workflow.params[1].name == "threshold"
        assert workflow.params[1].type_annotation == "float"

    def test_workflow_return_type(self, action_defs: dict[str, ActionDefinition]):
        """Test workflow return type annotation."""
        code = """
class ReturnTypeWorkflow(Workflow):
    async def run(self) -> dict[str, int]:
        result = await fetch_left()
        return {"value": result}
"""
        workflow = parse_workflow_class(code, action_defs)

        assert workflow.return_type == "dict[str, int]"


class TestSerializer:
    """Test IRSerializer output."""

    def test_serialize_simple(self, action_defs: dict[str, ActionDefinition]):
        """Test serialization of a simple workflow."""
        code = """
class SimpleWorkflow(Workflow):
    async def run(self) -> int:
        result = await fetch_left()
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        serializer = IRSerializer()
        text = serializer.serialize(workflow)

        assert "workflow SimpleWorkflow" in text
        assert "@example_module.fetch_left()" in text
        assert "return result" in text

    def test_serialize_with_locations(self, action_defs: dict[str, ActionDefinition]):
        """Test serialization with location comments."""
        code = """
class LocWorkflow(Workflow):
    async def run(self) -> int:
        result = await fetch_left()
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        serializer = IRSerializer(include_locations=True)
        text = serializer.serialize(workflow)

        assert "# line" in text

    def test_serialize_loop(self, action_defs: dict[str, ActionDefinition]):
        """Test serialization of loop."""
        code = """
class LoopWorkflow(Workflow):
    async def run(self, items: list) -> list:
        results = []
        for item in items:
            doubled = await double(value=item)
            results.append(doubled)
        return results
"""
        workflow = parse_workflow_class(code, action_defs)

        serializer = IRSerializer()
        text = serializer.serialize(workflow)

        assert "loop item in items" in text
        assert "yield doubled -> results" in text

    def test_serialize_conditional(self, action_defs: dict[str, ActionDefinition]):
        """Test serialization of conditional."""
        code = """
class CondWorkflow(Workflow):
    async def run(self, x: int) -> str:
        if x > 0:
            result = await evaluate_high(value=x)
        else:
            result = await evaluate_low(value=x)
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        serializer = IRSerializer()
        text = serializer.serialize(workflow)

        assert "branch if" in text
        assert "branch else" in text


class TestComplexWorkflows:
    """Test complex workflow patterns combining multiple constructs."""

    def test_gather_then_loop(self, action_defs: dict[str, ActionDefinition]):
        """Test gather followed by loop over results."""
        code = """
class GatherThenLoopWorkflow(Workflow):
    async def run(self) -> list:
        seeds = await asyncio.gather(fetch_number(idx=1), fetch_number(idx=2), fetch_number(idx=3))
        results = []
        for seed in seeds:
            processed = await double(value=seed)
            results.append(processed)
        return results
"""
        workflow = parse_workflow_class(code, action_defs)

        # Should have: gather, loop, return
        assert workflow.body[0].WhichOneof("kind") == "gather"
        assert workflow.body[1].WhichOneof("kind") == "loop"
        assert workflow.body[2].WhichOneof("kind") == "return_stmt"

    def test_multiple_gathers(
        self, action_defs: dict[str, ActionDefinition], module_index: ModuleIndex
    ):
        """Test multiple sequential gathers with Python computation."""
        code = """
class MultiGatherWorkflow(Workflow):
    async def run(self) -> list:
        batch1 = await asyncio.gather(fetch_number(idx=1), fetch_number(idx=2))
        batch2 = await asyncio.gather(fetch_number(idx=3), fetch_number(idx=4))
        combined = list(batch1) + list(batch2)
        return combined
"""
        workflow = parse_workflow_class(code, action_defs, module_index)

        # Should have: gather, gather, python_block, return
        assert workflow.body[0].WhichOneof("kind") == "gather"
        assert workflow.body[1].WhichOneof("kind") == "gather"
        assert workflow.body[2].WhichOneof("kind") == "python_block"
        assert workflow.body[3].WhichOneof("kind") == "return_stmt"
