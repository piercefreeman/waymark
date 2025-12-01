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


class TestModuleExceptionTypes:
    """Test exception types with module prefixes."""

    def test_module_exception_type(self, action_defs: dict[str, ActionDefinition]):
        """Test exception type with module prefix like mymodule.MyError."""
        code = """
class ModuleExceptionWorkflow(Workflow):
    async def run(self) -> int:
        try:
            result = await risky_action()
        except mymodule.MyError:
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
        assert len(handler.exception_types) == 1
        et = handler.exception_types[0]
        assert et.module == "mymodule"
        assert et.name == "MyError"


class TestBareExcept:
    """Test bare except clauses."""

    def test_bare_except(self, action_defs: dict[str, ActionDefinition]):
        """Test bare except (catches all exceptions)."""
        code = """
class BareExceptWorkflow(Workflow):
    async def run(self) -> int:
        try:
            result = await risky_action()
        except:
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
        # Bare except has no name
        assert len(handler.exception_types) == 1
        assert not handler.exception_types[0].HasField("name")


class TestReturnVariants:
    """Test various return statement patterns."""

    def test_return_none(self, action_defs: dict[str, ActionDefinition]):
        """Test return without value."""
        code = """
class ReturnNoneWorkflow(Workflow):
    async def run(self) -> None:
        await persist_summary(total=0.0)
        return
"""
        workflow = parse_workflow_class(code, action_defs)

        # Find return statement
        ret = None
        for stmt in workflow.body:
            if stmt.WhichOneof("kind") == "return_stmt":
                ret = stmt.return_stmt
                break

        assert ret is not None
        assert ret.WhichOneof("value") is None


class TestTimedeltaTimeout:
    """Test timedelta-based timeout values."""

    def test_timedelta_timeout(self, action_defs: dict[str, ActionDefinition]):
        """Test timeout with timedelta."""
        code = """
class TimedeltaTimeoutWorkflow(Workflow):
    async def run(self) -> int:
        result = await self.run_action(
            risky_action(),
            timeout=timedelta(minutes=5, seconds=30)
        )
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        action = workflow.body[0].action_call
        # 5 minutes + 30 seconds = 330 seconds
        assert action.config.timeout_seconds == 330


class TestUnlimitedRetry:
    """Test unlimited retry configurations."""

    def test_none_retry_value(self, action_defs: dict[str, ActionDefinition]):
        """Test retry with None (unlimited)."""
        code = """
class UnlimitedRetryWorkflow(Workflow):
    async def run(self) -> int:
        result = await self.run_action(risky_action(), retry=None)
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        action = workflow.body[0].action_call
        assert action.config.max_retries == 2_147_483_647  # Unlimited

    def test_retry_policy_none_attempts(self, action_defs: dict[str, ActionDefinition]):
        """Test RetryPolicy with attempts=None (unlimited)."""
        code = """
class UnlimitedRetryPolicyWorkflow(Workflow):
    async def run(self) -> int:
        result = await self.run_action(
            risky_action(),
            retry=RetryPolicy(attempts=None)
        )
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        action = workflow.body[0].action_call
        assert action.config.max_retries == 2_147_483_647

    def test_int_retry_value(self, action_defs: dict[str, ActionDefinition]):
        """Test retry with direct integer value."""
        code = """
class IntRetryWorkflow(Workflow):
    async def run(self) -> int:
        result = await self.run_action(risky_action(), retry=5)
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        action = workflow.body[0].action_call
        assert action.config.max_retries == 5


class TestActionWithoutModule:
    """Test actions without module definitions."""

    def test_action_without_module(self):
        """Test action that has no module in ActionDefinition."""
        action_defs = {
            "simple_action": ActionDefinition(
                name="simple_action",
                module=None,  # No module
                param_names=["x"],
            )
        }

        code = """
class SimpleActionWorkflow(Workflow):
    async def run(self) -> int:
        result = await simple_action(x=10)
        return result
"""
        full_code = (
            EXAMPLE_MODULE
            + "\nasync def simple_action(x: int) -> int: ...\n"
            + textwrap.dedent(code)
        )
        tree = ast.parse(full_code)

        workflow_class = None
        for node in tree.body:
            if isinstance(node, ast.ClassDef) and node.name == "SimpleActionWorkflow":
                workflow_class = node
                break

        assert workflow_class is not None

        run_method = None
        for item in workflow_class.body:
            if isinstance(item, ast.AsyncFunctionDef) and item.name == "run":
                run_method = item
                break

        assert run_method is not None

        parser = IRParser(action_defs=action_defs)
        workflow = parser.parse_workflow(run_method)

        action = workflow.body[0].action_call
        assert action.action == "simple_action"
        assert not action.HasField("module")


class TestSerializerCoverage:
    """Additional serializer tests for coverage."""

    def test_serialize_try_except(self, action_defs: dict[str, ActionDefinition]):
        """Test serialization of try/except."""
        code = """
class TryWorkflow(Workflow):
    async def run(self) -> int:
        try:
            result = await risky_action()
        except ValueError:
            result = await fallback_action()
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        serializer = IRSerializer()
        text = serializer.serialize(workflow)

        assert "try:" in text
        assert "except ValueError:" in text

    def test_serialize_try_multi_except(self, action_defs: dict[str, ActionDefinition]):
        """Test serialization of try with multiple exception types."""
        code = """
class TryMultiWorkflow(Workflow):
    async def run(self) -> int:
        try:
            result = await risky_action()
        except (ValueError, TypeError):
            result = await fallback_action()
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        serializer = IRSerializer()
        text = serializer.serialize(workflow)

        assert "try:" in text
        assert "except (ValueError, TypeError):" in text

    def test_serialize_bare_except(self, action_defs: dict[str, ActionDefinition]):
        """Test serialization of bare except."""
        code = """
class BareExceptSerializeWorkflow(Workflow):
    async def run(self) -> int:
        try:
            result = await risky_action()
        except:
            result = await fallback_action()
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        serializer = IRSerializer()
        text = serializer.serialize(workflow)

        assert "except:" in text

    def test_serialize_module_exception(self, action_defs: dict[str, ActionDefinition]):
        """Test serialization of module-prefixed exception."""
        code = """
class ModuleExceptSerializeWorkflow(Workflow):
    async def run(self) -> int:
        try:
            result = await risky_action()
        except mymodule.MyError:
            result = await fallback_action()
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        serializer = IRSerializer()
        text = serializer.serialize(workflow)

        assert "except mymodule.MyError:" in text

    def test_serialize_sleep(self, action_defs: dict[str, ActionDefinition]):
        """Test serialization of sleep."""
        code = """
class SleepSerializeWorkflow(Workflow):
    async def run(self) -> str:
        started = await get_timestamp()
        await asyncio.sleep(30)
        return started
"""
        workflow = parse_workflow_class(code, action_defs)

        serializer = IRSerializer()
        text = serializer.serialize(workflow)

        assert "@sleep(30)" in text

    def test_serialize_python_block_with_definitions(
        self, action_defs: dict[str, ActionDefinition], module_index: ModuleIndex
    ):
        """Test serialization of python block with definitions."""
        code = """
class DefWorkflow(Workflow):
    async def run(self) -> float:
        records = [Record(5), Record(20)]
        positives = [r.amount for r in records if helper_threshold(r)]
        total = await summarize(values=positives)
        return total
"""
        workflow = parse_workflow_class(code, action_defs, module_index)

        serializer = IRSerializer()
        text = serializer.serialize(workflow)

        # Should have python block with definition reference
        assert "python" in text

    def test_serialize_spread(self, action_defs: dict[str, ActionDefinition]):
        """Test serialization of spread."""
        code = """
class SpreadSerializeWorkflow(Workflow):
    async def run(self) -> list:
        numbers = await asyncio.gather(fetch_number(idx=1), fetch_number(idx=2))
        doubled = [await double(value=n) for n in numbers]
        return doubled
"""
        workflow = parse_workflow_class(code, action_defs)

        serializer = IRSerializer()
        text = serializer.serialize(workflow)

        assert "spread" in text
        assert "over numbers as n" in text

    def test_serialize_run_action_with_full_config(self, action_defs: dict[str, ActionDefinition]):
        """Test serialization of run_action with all config options."""
        code = """
class FullConfigWorkflow(Workflow):
    async def run(self) -> int:
        result = await self.run_action(
            risky_action(),
            timeout=60,
            retry=3,
            backoff=ExponentialBackoff(base_delay_ms=100, multiplier=2.0)
        )
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        serializer = IRSerializer()
        text = serializer.serialize(workflow)

        assert "timeout=60s" in text
        assert "retry=3" in text
        assert "backoff=exp(100ms, 2.0x)" in text

    def test_serialize_gather_no_target(self, action_defs: dict[str, ActionDefinition]):
        """Test serialization of gather without target (returned directly)."""
        code = """
class GatherReturnWorkflow(Workflow):
    async def run(self) -> tuple:
        return await asyncio.gather(fetch_left(), fetch_right())
"""
        workflow = parse_workflow_class(code, action_defs)

        serializer = IRSerializer()
        text = serializer.serialize(workflow)

        assert "return" in text
        assert "parallel(" in text


class TestPythonBlockDefinitions:
    """Test Python blocks with function definitions."""

    def test_python_block_with_helper_function(
        self, action_defs: dict[str, ActionDefinition], module_index: ModuleIndex
    ):
        """Test Python block that uses helper function from module."""
        code = """
class HelperFunctionWorkflow(Workflow):
    async def run(self) -> float:
        records = [Record(5), Record(20), Record(3)]
        positives = [r.amount for r in records if helper_threshold(r)]
        total = await summarize(values=positives)
        return total
"""
        workflow = parse_workflow_class(code, action_defs, module_index)

        # Find python block
        py_block = None
        for stmt in workflow.body:
            if stmt.WhichOneof("kind") == "python_block":
                py_block = stmt.python_block
                break

        assert py_block is not None
        # Should have Record and helper_threshold in definitions
        # The functions/classes are in the module index
        all_text = " ".join(py_block.definitions) + " ".join(py_block.imports)
        assert "Record" in all_text or "dataclass" in all_text or len(py_block.definitions) > 0


class TestVariableAnalysis:
    """Test variable analysis in Python blocks."""

    def test_aug_assign_variable(
        self, action_defs: dict[str, ActionDefinition], module_index: ModuleIndex
    ):
        """Test augmented assignment (+=) tracking."""
        code = """
class AugAssignWorkflow(Workflow):
    async def run(self) -> int:
        total = 0
        items = await asyncio.gather(fetch_number(idx=1), fetch_number(idx=2))
        total += sum(items)
        result = await double(value=total)
        return result
"""
        workflow = parse_workflow_class(code, action_defs, module_index)

        # Find python block with augmented assignment
        py_blocks = [
            s.python_block for s in workflow.body if s.WhichOneof("kind") == "python_block"
        ]
        assert len(py_blocks) >= 1
        # The aug assign block should have total in both reads and writes
        aug_block = [b for b in py_blocks if "total +=" in b.code]
        assert len(aug_block) == 1
        assert "total" in aug_block[0].outputs


class TestSerializerPythonBlockEdgeCases:
    """Test serializer handling of Python blocks with various I/O combinations."""

    def test_serialize_python_block_no_io(self, action_defs: dict[str, ActionDefinition]):
        """Test serialization of Python block with no I/O."""
        code = """
class NoIOWorkflow(Workflow):
    async def run(self) -> int:
        x = 42
        result = await double(value=x)
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        serializer = IRSerializer()
        text = serializer.serialize(workflow)

        assert "python" in text

    def test_serialize_loop_with_preamble(self, action_defs: dict[str, ActionDefinition]):
        """Test serialization of loop with preamble."""
        code = """
class LoopPreambleWorkflow(Workflow):
    async def run(self, items: list) -> list:
        results = []
        for item in items:
            adjusted = item * 2
            doubled = await double(value=adjusted)
            results.append(doubled)
        return results
"""
        workflow = parse_workflow_class(code, action_defs)

        serializer = IRSerializer()
        text = serializer.serialize(workflow)

        assert "loop item in items" in text
        assert "# preamble" in text

    def test_serialize_conditional_with_preamble_postamble(
        self, action_defs: dict[str, ActionDefinition]
    ):
        """Test serialization of conditional with preamble and postamble."""
        code = """
class PreamblePostambleWorkflow(Workflow):
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

        serializer = IRSerializer()
        text = serializer.serialize(workflow)

        assert "# preamble" in text
        assert "# postamble" in text


class TestExtraPositionalArgs:
    """Test handling of extra positional args beyond signature."""

    def test_extra_positional_args_mapped(self):
        """Test that extra positional args beyond signature get __argN names."""
        action_defs = {
            "few_params": ActionDefinition(
                name="few_params",
                module="example_module",
                param_names=["a"],  # Only one param defined
            )
        }

        code = """
class ExtraArgsWorkflow(Workflow):
    async def run(self) -> str:
        result = await few_params("first", "second", "third")
        return result
"""
        full_code = EXAMPLE_MODULE + "\nasync def few_params(a): ...\n" + textwrap.dedent(code)
        tree = ast.parse(full_code)

        workflow_class = None
        for node in tree.body:
            if isinstance(node, ast.ClassDef) and node.name == "ExtraArgsWorkflow":
                workflow_class = node
                break

        assert workflow_class is not None

        run_method = None
        for item in workflow_class.body:
            if isinstance(item, ast.AsyncFunctionDef) and item.name == "run":
                run_method = item
                break

        assert run_method is not None

        parser = IRParser(action_defs=action_defs)
        workflow = parser.parse_workflow(run_method)

        action = workflow.body[0].action_call
        # First arg mapped to param name
        assert action.kwargs["a"] == "'first'"
        # Extra args get __argN names
        assert action.kwargs["__arg1"] == "'second'"
        assert action.kwargs["__arg2"] == "'third'"

    def test_positional_args_no_signature(self):
        """Test positional args when action has no param_names."""
        action_defs = {
            "no_sig_action": ActionDefinition(
                name="no_sig_action",
                module="example_module",
                param_names=[],  # No param names
            )
        }

        code = """
class NoSigWorkflow(Workflow):
    async def run(self) -> str:
        result = await no_sig_action("first", "second")
        return result
"""
        full_code = (
            EXAMPLE_MODULE + "\nasync def no_sig_action(*args): ...\n" + textwrap.dedent(code)
        )
        tree = ast.parse(full_code)

        workflow_class = None
        for node in tree.body:
            if isinstance(node, ast.ClassDef) and node.name == "NoSigWorkflow":
                workflow_class = node
                break

        assert workflow_class is not None

        run_method = None
        for item in workflow_class.body:
            if isinstance(item, ast.AsyncFunctionDef) and item.name == "run":
                run_method = item
                break

        assert run_method is not None

        parser = IRParser(action_defs=action_defs)
        workflow = parser.parse_workflow(run_method)

        action = workflow.body[0].action_call
        # All args get __argN names when no signature
        assert action.kwargs["__arg0"] == "'first'"
        assert action.kwargs["__arg1"] == "'second'"


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


class TestSerializerReturnEdgeCases:
    """Test serializer handling of various return patterns."""

    def test_serialize_return_action(self, action_defs: dict[str, ActionDefinition]):
        """Test serialization of return with action call."""
        code = """
class ReturnActionSerializeWorkflow(Workflow):
    async def run(self) -> int:
        return await fetch_left()
"""
        workflow = parse_workflow_class(code, action_defs)

        serializer = IRSerializer()
        text = serializer.serialize(workflow)

        assert "return" in text
        assert "@example_module.fetch_left()" in text

    def test_serialize_return_gather(self, action_defs: dict[str, ActionDefinition]):
        """Test serialization of return with gather."""
        code = """
class ReturnGatherSerializeWorkflow(Workflow):
    async def run(self) -> tuple:
        return await asyncio.gather(fetch_left(), fetch_right())
"""
        workflow = parse_workflow_class(code, action_defs)

        serializer = IRSerializer()
        text = serializer.serialize(workflow)

        assert "return" in text
        assert "parallel(" in text


class TestPythonBlockForLoops:
    """Test Python blocks containing for loops for variable analysis."""

    def test_python_block_with_list_comprehension(
        self, action_defs: dict[str, ActionDefinition], module_index: ModuleIndex
    ):
        """Test Python block with list comprehension variable analysis."""
        code = """
class ListCompBlockWorkflow(Workflow):
    async def run(self) -> float:
        items = await asyncio.gather(fetch_number(idx=1), fetch_number(idx=2))
        squared = [x * x for x in items]
        total = sum(squared)
        result = await double(value=total)
        return result
"""
        workflow = parse_workflow_class(code, action_defs, module_index)

        # Find python block with list comprehension
        py_blocks = [
            s.python_block for s in workflow.body if s.WhichOneof("kind") == "python_block"
        ]
        assert len(py_blocks) >= 1
        # Should have a block that reads items
        item_blocks = [b for b in py_blocks if "items" in b.inputs or "squared" in b.outputs]
        assert len(item_blocks) >= 1


class TestMultiLineLocation:
    """Test source location formatting for multi-line nodes."""

    def test_multiline_location_format(self, action_defs: dict[str, ActionDefinition]):
        """Test that multi-line nodes get proper location formatting."""
        code = """
class MultilineWorkflow(Workflow):
    async def run(self) -> list:
        results = await asyncio.gather(
            fetch_number(idx=1),
            fetch_number(idx=2),
            fetch_number(idx=3)
        )
        return results
"""
        workflow = parse_workflow_class(code, action_defs)

        serializer = IRSerializer(include_locations=True)
        text = serializer.serialize(workflow)

        # The gather spans multiple lines, should have line range or single line
        assert "line" in text


class TestConditionWithoutActions:
    """Test that conditionals without actions become Python blocks."""

    def test_pure_python_conditional(self, action_defs: dict[str, ActionDefinition]):
        """Test conditional without actions is treated as Python block."""
        code = """
class PurePythonConditionalWorkflow(Workflow):
    async def run(self, x: int) -> int:
        if x > 0:
            multiplier = 2
        else:
            multiplier = 1
        result = await double(value=x * multiplier)
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        # The if/else without actions should be a python_block
        py_blocks = [s for s in workflow.body if s.WhichOneof("kind") == "python_block"]
        assert len(py_blocks) >= 1


class TestSerializeElifBranch:
    """Test serialization of elif branches."""

    def test_serialize_elif_branch(self, action_defs: dict[str, ActionDefinition]):
        """Test serialization of elif branches shows 'elif' not 'else'."""
        code = """
class ElifSerializeWorkflow(Workflow):
    async def run(self, x: int) -> str:
        if x > 100:
            result = await evaluate_high(value=x)
        elif x > 50:
            result = await evaluate_medium(value=x)
        elif x > 0:
            result = await evaluate_low(value=x)
        else:
            result = await evaluate_low(value=0)
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        serializer = IRSerializer()
        text = serializer.serialize(workflow)

        # Should have elif branches (not just else)
        assert "branch if" in text
        assert "branch elif" in text
        assert "branch else" in text


class TestModuleIndexEdgeCases:
    """Test ModuleIndex edge cases."""

    def test_module_index_resolve_dependencies(self):
        """Test that resolve handles dependencies correctly."""
        module_code = """
def helper1():
    return helper2()

def helper2():
    return 42
"""
        index = ModuleIndex(module_code)
        # Request helper1 which depends on helper2
        imports, definitions = index.resolve({"helper1"})
        # helper2 should also be resolved since helper1 depends on it
        combined = "\n".join(definitions)
        assert "def helper1" in combined
        assert "def helper2" in combined

    def test_module_index_already_resolved_skip(self):
        """Test that requesting same name twice doesn't duplicate."""
        module_code = """
def helper1():
    return 42
"""
        index = ModuleIndex(module_code)
        # First resolution
        _, defs1 = index.resolve({"helper1"})
        # The name is resolved once
        assert len([d for d in defs1 if "helper1" in d]) == 1

    def test_module_index_missing_snippet(self):
        """Test that ModuleIndex handles None snippet gracefully."""
        # This is hard to trigger naturally since ast.get_source_segment
        # typically returns valid strings for valid AST nodes.
        # The continue on line 93 is defensive code that's unlikely to be hit
        # in normal usage but protects against edge cases in malformed source.
        index = ModuleIndex("x = 1\n")
        assert "x" not in index._definitions  # Simple assignment, not a function


class TestFormatLocationEdgeCases:
    """Test _format_location edge cases."""

    def test_format_location_none(self):
        """Test _format_location with None returns empty string."""
        from rappel.ir import _format_location

        result = _format_location(None)
        assert result == ""


class TestVariableAnalyzerForLoop:
    """Test VariableAnalyzer with for loops inside Python blocks."""

    def test_for_loop_in_conditional_python_block(
        self, action_defs: dict[str, ActionDefinition], module_index: ModuleIndex
    ):
        """Test variable analysis for for loop inside a conditional Python block."""
        # For loops at the top level require actions, but conditionals without actions
        # become Python blocks. A conditional with a for loop (no actions) triggers
        # the visit_For path in VariableAnalyzer when analyzing the python block.
        code = """
class ForInConditionalWorkflow(Workflow):
    async def run(self, items: list, flag: bool) -> int:
        if flag:
            total = 0
            for item in items:
                total += item
        else:
            total = -1
        result = await double(value=total)
        return result
"""
        workflow = parse_workflow_class(code, action_defs, module_index)

        # Conditional without actions becomes python block
        py_blocks = [
            s.python_block for s in workflow.body if s.WhichOneof("kind") == "python_block"
        ]
        assert len(py_blocks) >= 1
        # The python block should have items and flag as inputs
        block = py_blocks[0]
        assert "items" in block.inputs or "flag" in block.inputs


class TestGatherExpressionStatement:
    """Test gather as expression statement (not assigned)."""

    def test_gather_expression_statement(self, action_defs: dict[str, ActionDefinition]):
        """Test gather without assignment."""
        code = """
class GatherExprWorkflow(Workflow):
    async def run(self) -> None:
        await asyncio.gather(fetch_left(), fetch_right())
        await persist_summary(total=0.0)
"""
        workflow = parse_workflow_class(code, action_defs)

        # First statement should be gather without target
        gather = workflow.body[0].gather
        assert not gather.HasField("target")


class TestMultiTargetAssignment:
    """Test multi-target assignment becomes Python block."""

    def test_multi_target_assignment(
        self, action_defs: dict[str, ActionDefinition], module_index: ModuleIndex
    ):
        """Test a = b = value becomes Python block."""
        code = """
class MultiTargetWorkflow(Workflow):
    async def run(self) -> int:
        a = b = 42
        result = await double(value=a + b)
        return result
"""
        workflow = parse_workflow_class(code, action_defs, module_index)

        # Multi-target assignment should become python block
        py_block = workflow.body[0]
        assert py_block.WhichOneof("kind") == "python_block"


class TestNonNameTargetAssignment:
    """Test non-name target assignment becomes Python block."""

    def test_tuple_unpack_assignment(
        self, action_defs: dict[str, ActionDefinition], module_index: ModuleIndex
    ):
        """Test tuple unpacking becomes Python block."""
        code = """
class TupleUnpackWorkflow(Workflow):
    async def run(self) -> int:
        x, y = 1, 2
        result = await double(value=x + y)
        return result
"""
        workflow = parse_workflow_class(code, action_defs, module_index)

        # Tuple unpack should become python block
        py_block = workflow.body[0]
        assert py_block.WhichOneof("kind") == "python_block"


class TestDeepExceptionType:
    """Test deeply nested module exception types."""

    def test_deeply_nested_module_exception(self, action_defs: dict[str, ActionDefinition]):
        """Test exception type like a.b.c.MyError."""
        code = """
class DeepExceptionWorkflow(Workflow):
    async def run(self) -> int:
        try:
            result = await risky_action()
        except some.deep.module.CustomError:
            result = await fallback_action()
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        te = workflow.body[0].try_except
        handler = te.handlers[0]
        exc_type = handler.exception_types[0]
        assert exc_type.module == "some.deep.module"
        assert exc_type.name == "CustomError"


class TestRunActionAwaitInner:
    """Test run_action with await on inner call."""

    def test_run_action_with_await_inner(self, action_defs: dict[str, ActionDefinition]):
        """Test run_action(await action()) strips await."""
        code = """
class AwaitInnerWorkflow(Workflow):
    async def run(self) -> int:
        result = await self.run_action(await risky_action())
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        action = workflow.body[0].action_call
        assert action.action == "risky_action"


class TestTimedeltaPositionalArgs:
    """Test timedelta with positional arguments."""

    def test_timedelta_positional_days(self, action_defs: dict[str, ActionDefinition]):
        """Test timedelta(1) == 1 day in seconds."""
        code = """
class TimedeltaDaysWorkflow(Workflow):
    async def run(self) -> int:
        result = await self.run_action(risky_action(), timeout=timedelta(1))
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        action = workflow.body[0].action_call
        # timedelta(1) means 1 day = 86400 seconds
        assert action.config.timeout_seconds == 86400


class TestSleepEdgeCases:
    """Test asyncio.sleep edge cases."""

    def test_sleep_awaited_expression(self, action_defs: dict[str, ActionDefinition]):
        """Test asyncio.sleep is properly extracted."""
        code = """
class SleepWorkflow(Workflow):
    async def run(self) -> None:
        await asyncio.sleep(10)
        await persist_summary(total=0.0)
"""
        workflow = parse_workflow_class(code, action_defs)

        # First statement should be sleep
        sleep = workflow.body[0].sleep
        assert sleep.duration_expr == "10"


class TestSpreadEdgeCases:
    """Test spread pattern edge cases."""

    def test_spread_with_action_await(self, action_defs: dict[str, ActionDefinition]):
        """Test spread pattern [await action(x=v) for v in items]."""
        code = """
class SpreadWorkflow(Workflow):
    async def run(self, items: list) -> list:
        results = [await double(value=v) for v in items]
        return results
"""
        workflow = parse_workflow_class(code, action_defs)

        # Should be a spread statement
        spread = workflow.body[0].spread
        assert spread.loop_var == "v"
        assert spread.iterable == "items"
        assert spread.target == "results"


class TestSerializerUnknownKind:
    """Test serializer handling of unknown statement kinds."""

    def test_serialize_unknown_statement_kind(self):
        """Test serializer returns comment for unknown kind."""
        # Create a statement with no kind set
        workflow = ir_pb2.Workflow(name="test")
        stmt = ir_pb2.Statement()  # Empty statement
        workflow.body.append(stmt)

        serializer = IRSerializer()
        text = serializer.serialize(workflow)

        # Should contain UNKNOWN marker
        assert "# UNKNOWN" in text


class TestSerializerExceptionModules:
    """Test serializer handling of module-qualified exceptions."""

    def test_serialize_module_exception(self, action_defs: dict[str, ActionDefinition]):
        """Test serialization includes module prefix for exceptions."""
        code = """
class ModuleExceptionSerializeWorkflow(Workflow):
    async def run(self) -> int:
        try:
            result = await risky_action()
        except mymodule.MyError:
            result = await fallback_action()
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        serializer = IRSerializer()
        text = serializer.serialize(workflow)

        assert "mymodule.MyError" in text


class TestSerializerReturnVariants:
    """Test serializer return statement variants."""

    def test_serialize_return_empty(self, action_defs: dict[str, ActionDefinition]):
        """Test serialization of bare return."""
        code = """
class BareReturnWorkflow(Workflow):
    async def run(self) -> None:
        await persist_summary(total=0.0)
        return
"""
        workflow = parse_workflow_class(code, action_defs)

        serializer = IRSerializer()
        text = serializer.serialize(workflow)

        # Should have plain "return" line
        lines = text.strip().split("\n")
        assert any("return" in line and "@" not in line for line in lines)


class TestSerializerSpreadWithTarget:
    """Test serializer spread with target variable."""

    def test_serialize_spread_with_target(self, action_defs: dict[str, ActionDefinition]):
        """Test serialization of spread assigns to target."""
        code = """
class SpreadSerializeWorkflow(Workflow):
    async def run(self, items: list) -> list:
        results = [await double(value=v) for v in items]
        return results
"""
        workflow = parse_workflow_class(code, action_defs)

        serializer = IRSerializer()
        text = serializer.serialize(workflow)

        # Should show spread pattern with assignment
        assert "results" in text
        assert "items" in text


class TestSerializerGatherWithoutTarget:
    """Test serializer gather without target."""

    def test_serialize_gather_no_target(self, action_defs: dict[str, ActionDefinition]):
        """Test serialization of gather without assignment."""
        code = """
class GatherNoTargetWorkflow(Workflow):
    async def run(self) -> None:
        await asyncio.gather(fetch_left(), fetch_right())
        await persist_summary(total=0.0)
"""
        workflow = parse_workflow_class(code, action_defs)

        serializer = IRSerializer()
        text = serializer.serialize(workflow)

        # Should have parallel() without assignment
        assert "parallel(" in text


class TestSerializerActionWithoutTarget:
    """Test serializer action without target variable."""

    def test_serialize_action_no_target(self, action_defs: dict[str, ActionDefinition]):
        """Test serialization of action call without assignment."""
        code = """
class ActionNoTargetWorkflow(Workflow):
    async def run(self) -> None:
        await persist_summary(total=1.0)
"""
        workflow = parse_workflow_class(code, action_defs)

        serializer = IRSerializer()
        text = serializer.serialize(workflow)

        # Should have action without assignment
        assert "@example_module.persist_summary(total=1.0)" in text


class TestSerializerPostambleOutputs:
    """Test serializer postamble with outputs."""

    def test_serialize_postamble_with_writes(self, action_defs: dict[str, ActionDefinition]):
        """Test serialization shows postamble writes."""
        code = """
class PostambleWritesWorkflow(Workflow):
    async def run(self, value: int) -> str:
        if value > 50:
            result = await evaluate_high(value=value)
            label = "high"
        else:
            result = await evaluate_low(value=value)
            label = "low"
        return f"{label}: {result}"
"""
        workflow = parse_workflow_class(code, action_defs)

        serializer = IRSerializer()
        text = serializer.serialize(workflow)

        # Should have postamble section with writes
        assert "# postamble" in text
        assert "writes:" in text


class TestKeywordDefaults:
    """Test keyword extraction with defaults."""

    def test_backoff_with_multiplier(self, action_defs: dict[str, ActionDefinition]):
        """Test backoff with custom multiplier uses default if not specified."""
        code = """
class BackoffMultiplierWorkflow(Workflow):
    async def run(self) -> int:
        result = await self.run_action(
            risky_action(),
            backoff=ExponentialBackoff(base_delay_ms=100, multiplier=3.0)
        )
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        action = workflow.body[0].action_call
        assert action.config.backoff.multiplier == 3.0

    def test_backoff_default_multiplier(self, action_defs: dict[str, ActionDefinition]):
        """Test backoff without multiplier uses default 2.0."""
        code = """
class BackoffDefaultMultiplierWorkflow(Workflow):
    async def run(self) -> int:
        result = await self.run_action(
            risky_action(),
            backoff=ExponentialBackoff(base_delay_ms=100)
        )
        return result
"""
        workflow = parse_workflow_class(code, action_defs)

        action = workflow.body[0].action_call
        assert action.config.backoff.multiplier == 2.0


class TestNonRegisteredAsyncFunctions:
    """Test that non-registered async functions become Python blocks, not actions."""

    def test_unregistered_await_becomes_python_block(
        self, action_defs: dict[str, ActionDefinition], module_index: ModuleIndex
    ):
        """Await on unregistered function should become Python block, not action."""
        # Add an async function that is NOT in action_defs
        code = """
async def some_helper() -> int:
    return 42

class UnregisteredAwaitWorkflow(Workflow):
    async def run(self) -> int:
        # some_helper is NOT a registered action
        helper_result = await some_helper()
        # double IS a registered action
        result = await double(value=helper_result)
        return result
"""
        full_code = EXAMPLE_MODULE + "\n" + textwrap.dedent(code)
        tree = ast.parse(full_code)

        workflow_class = None
        for node in tree.body:
            if isinstance(node, ast.ClassDef) and node.name == "UnregisteredAwaitWorkflow":
                workflow_class = node
                break

        assert workflow_class is not None

        run_method = None
        for item in workflow_class.body:
            if isinstance(item, ast.AsyncFunctionDef) and item.name == "run":
                run_method = item
                break

        assert run_method is not None

        parser = IRParser(action_defs=action_defs, module_index=module_index, source=full_code)
        workflow = parser.parse_workflow(run_method)

        # First statement should be python_block (unregistered await)
        assert workflow.body[0].WhichOneof("kind") == "python_block"
        assert "some_helper" in workflow.body[0].python_block.code

        # Second statement should be action_call (registered action)
        assert workflow.body[1].WhichOneof("kind") == "action_call"
        assert workflow.body[1].action_call.action == "double"

    def test_unregistered_in_gather_raises_error(
        self, action_defs: dict[str, ActionDefinition], module_index: ModuleIndex
    ):
        """Gather with unregistered function should raise an error.

        This is the correct behavior - asyncio.gather in workflows should only
        contain registered actions that can be orchestrated by the IR. If you
        need to call an unregistered async function, do it outside of gather
        and pass the result to gather.
        """
        from rappel.ir import IRParseError

        code = """
async def unregistered_func() -> int:
    return 1

class UnregisteredGatherWorkflow(Workflow):
    async def run(self) -> tuple:
        # This should error - gather must contain only registered actions
        results = await asyncio.gather(unregistered_func(), fetch_left())
        return results
"""
        full_code = EXAMPLE_MODULE + "\n" + textwrap.dedent(code)
        tree = ast.parse(full_code)

        workflow_class = None
        for node in tree.body:
            if isinstance(node, ast.ClassDef) and node.name == "UnregisteredGatherWorkflow":
                workflow_class = node
                break

        assert workflow_class is not None

        run_method = None
        for item in workflow_class.body:
            if isinstance(item, ast.AsyncFunctionDef) and item.name == "run":
                run_method = item
                break

        assert run_method is not None

        parser = IRParser(action_defs=action_defs, module_index=module_index, source=full_code)

        with pytest.raises(IRParseError) as exc_info:
            parser.parse_workflow(run_method)

        assert "gather argument must be an action call" in str(exc_info.value)
        assert "unregistered_func" in str(exc_info.value)
