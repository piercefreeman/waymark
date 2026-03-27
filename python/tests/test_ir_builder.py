"""Tests for IR builder functionality.

This module contains comprehensive tests for the Python AST to IR conversion.
The tests are organized by category:
- TestAsyncioSleepDetection: asyncio.sleep() -> SleepStmt
- TestAsyncioGatherDetection: asyncio.gather() -> ParallelBlock/SpreadAction
- TestPolicyParsing: RetryPolicy and TimeoutPolicy extraction
- TestForLoopConversion: for loop IR generation
- TestWhileLoopConversion: while loop IR generation
- TestConditionalConversion: if/elif/else IR generation
- TestTryExceptConversion: try/except IR generation
- TestActionCallExtraction: action call detection and kwargs
- TestWorkflowHelperMethods: self.method() -> FunctionCall
"""

from __future__ import annotations

from collections.abc import Iterator
from enum import Enum
from typing import List, Optional

from proto import ast_pb2 as ir

# Global variable for test_global_statement_raises_error test
some_var: int = 0


class ExampleStatus(Enum):
    READY = "ready"
    RETRIES = 2


def iter_all_statements(program: ir.Program) -> Iterator[ir.Statement]:
    def iter_block(block: ir.Block) -> Iterator[ir.Statement]:
        yield from iter_block_statements(block)

    for fn in program.functions:
        yield from iter_block(fn.body)


def iter_block_statements(block: ir.Block) -> Iterator[ir.Statement]:
    for stmt in block.statements:
        yield stmt

        if stmt.HasField("conditional"):
            cond = stmt.conditional
            if cond.HasField("if_branch") and cond.if_branch.HasField("block_body"):
                yield from iter_block_statements(cond.if_branch.block_body)
            for elif_branch in cond.elif_branches:
                if elif_branch.HasField("block_body"):
                    yield from iter_block_statements(elif_branch.block_body)
            if cond.HasField("else_branch") and cond.else_branch.HasField("block_body"):
                yield from iter_block_statements(cond.else_branch.block_body)

        if stmt.HasField("for_loop") and stmt.for_loop.HasField("block_body"):
            yield from iter_block_statements(stmt.for_loop.block_body)

        if stmt.HasField("while_loop") and stmt.while_loop.HasField("block_body"):
            yield from iter_block_statements(stmt.while_loop.block_body)

        if stmt.HasField("try_except"):
            te = stmt.try_except
            if te.HasField("try_block"):
                yield from iter_block_statements(te.try_block)
            for handler in te.handlers:
                if handler.HasField("block_body"):
                    yield from iter_block_statements(handler.block_body)


class TestAsyncioSleepDetection:
    """Test that asyncio.sleep is detected and converted to SleepStmt."""

    def _find_sleep_stmt(self, program: ir.Program) -> ir.SleepStmt | None:
        """Find a sleep statement in the program."""
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("sleep_stmt"):
                    return stmt.sleep_stmt
        return None

    def _get_duration_expr(self, sleep_stmt: ir.SleepStmt) -> ir.Expr | None:
        """Get the duration expression from a sleep statement."""
        if sleep_stmt.HasField("duration"):
            return sleep_stmt.duration
        return None

    def test_asyncio_dot_sleep_pattern(self) -> None:
        """Test: import asyncio; asyncio.sleep(1)"""
        from tests.fixtures_sleep.sleep_import_asyncio import SleepImportAsyncioWorkflow

        program = SleepImportAsyncioWorkflow.workflow_ir()

        sleep_stmt = self._find_sleep_stmt(program)
        assert sleep_stmt is not None, "Expected sleep statement in IR"

        duration = self._get_duration_expr(sleep_stmt)
        assert duration is not None, "Expected duration expression"
        assert duration.HasField("literal"), "Expected literal value"
        assert duration.literal.int_value == 1

    def test_from_asyncio_import_sleep_pattern(self) -> None:
        """Test: from asyncio import sleep; sleep(2)"""
        from tests.fixtures_sleep.sleep_from_import import SleepFromImportWorkflow

        program = SleepFromImportWorkflow.workflow_ir()

        sleep_stmt = self._find_sleep_stmt(program)
        assert sleep_stmt is not None, "Expected sleep statement in IR"

        duration = self._get_duration_expr(sleep_stmt)
        assert duration is not None, "Expected duration expression"
        assert duration.HasField("literal"), "Expected literal value"
        assert duration.literal.int_value == 2

    def test_from_asyncio_import_sleep_as_alias_pattern(self) -> None:
        """Test: from asyncio import sleep as async_sleep; async_sleep(3)"""
        from tests.fixtures_sleep.sleep_aliased_import import SleepAliasedImportWorkflow

        program = SleepAliasedImportWorkflow.workflow_ir()

        sleep_stmt = self._find_sleep_stmt(program)
        assert sleep_stmt is not None, "Expected sleep statement in IR"

        duration = self._get_duration_expr(sleep_stmt)
        assert duration is not None, "Expected duration expression"
        assert duration.HasField("literal"), "Expected literal value"
        assert duration.literal.int_value == 3


class TestVariableReferenceValidation:
    """Tests for workflow input extraction in IR building."""

    def test_kwonly_inputs_included(self) -> None:
        from waymark import action, workflow
        from waymark.workflow import Workflow

        @action
        async def echo(value: float | None) -> None:
            return None

        @workflow
        class KwOnlyWorkflow(Workflow):
            async def run(
                self,
                *,
                latitude: float | None = None,
                longitude: float | None = None,
            ) -> None:
                await echo(latitude)

        program = KwOnlyWorkflow.workflow_ir()
        main = next(fn for fn in program.functions if fn.name == "main")
        assert "latitude" in list(main.io.inputs)
        assert "longitude" in list(main.io.inputs)


class TestAnnotatedAssignment:
    """Tests for annotated assignment handling in IR building."""

    def test_ann_assign_generates_assignment(self) -> None:
        from waymark import action, workflow
        from waymark.workflow import Workflow

        @action
        async def echo(value: int | None) -> None:
            return None

        @workflow
        class AnnAssignWorkflow(Workflow):
            async def run(self) -> None:
                pagination_state: int | None = None
                await echo(pagination_state)

        program = AnnAssignWorkflow.workflow_ir()
        assignments = [
            stmt.assignment for stmt in iter_all_statements(program) if stmt.HasField("assignment")
        ]
        matches = [
            assignment for assignment in assignments if "pagination_state" in assignment.targets
        ]
        assert len(matches) == 1, "Expected annotated assignment to create Assignment"
        value = matches[0].value
        assert value.HasField("literal")
        assert value.literal.is_none


class TestEnumAttribute:
    """Tests for enum attribute handling in IR building."""

    def test_enum_attribute_is_literal(self) -> None:
        from waymark import action, workflow
        from waymark.workflow import Workflow

        @action
        async def echo(value: object) -> None:
            return None

        @workflow
        class EnumAttributeWorkflow(Workflow):
            async def run(self) -> None:
                await echo(ExampleStatus.READY)
                await echo(ExampleStatus.RETRIES)

        program = EnumAttributeWorkflow.workflow_ir()
        calls = [
            stmt.action_call
            for stmt in iter_all_statements(program)
            if stmt.HasField("action_call") and stmt.action_call.action_name == "echo"
        ]
        assert len(calls) == 2

        literal_values: list[object] = []
        for call in calls:
            kwarg = next(kw for kw in call.kwargs if kw.name == "value")
            literal = kwarg.value.literal
            kind = literal.WhichOneof("value")
            if kind == "string_value":
                literal_values.append(literal.string_value)
            elif kind == "int_value":
                literal_values.append(literal.int_value)
            else:
                raise AssertionError(f"Unexpected enum literal kind: {kind}")

        assert "ready" in literal_values
        assert 2 in literal_values


class TestPolicyParsing:
    """Test that retry and timeout policies are parsed from run_action calls."""

    def _find_action_with_policies(
        self, program: ir.Program, action_name: str
    ) -> ir.ActionCall | None:
        """Find an action call by name, searching in all contexts."""
        for stmt in iter_all_statements(program):
            if stmt.HasField("action_call") and stmt.action_call.action_name == action_name:
                return stmt.action_call
            if (
                stmt.HasField("assignment")
                and stmt.assignment.value.HasField("action_call")
                and stmt.assignment.value.action_call.action_name == action_name
            ):
                return stmt.assignment.value.action_call
        return None

    def test_timeout_policy_with_timedelta(self) -> None:
        """Test: self.run_action(action(), timeout=timedelta(seconds=2))"""
        from tests.fixtures_policy.integration_crash_recovery import CrashRecoveryWorkflow

        program = CrashRecoveryWorkflow.workflow_ir()

        # Find step_one which has timeout=timedelta(seconds=2)
        action = self._find_action_with_policies(program, "step_one")
        assert action is not None, "Expected @step_one action"
        assert len(action.policies) == 1, "Expected 1 policy"

        policy = action.policies[0]
        assert policy.HasField("timeout"), "Expected timeout policy"
        assert policy.timeout.timeout.seconds == 2

    def test_retry_policy_with_attempts(self) -> None:
        """Test: self.run_action(action(), retry=RetryPolicy(attempts=1))"""
        from tests.fixtures_policy.integration_exception_custom import ExceptionCustomWorkflow

        program = ExceptionCustomWorkflow.workflow_ir()

        # Find explode_custom which has retry=RetryPolicy(attempts=1)
        action = self._find_action_with_policies(program, "explode_custom")
        assert action is not None, "Expected @explode_custom action"
        assert len(action.policies) == 1, "Expected 1 policy"

        policy = action.policies[0]
        assert policy.HasField("retry"), "Expected retry policy"
        # attempts=1 means 1 total execution, so max_retries=0 (no retries)
        assert policy.retry.max_retries == 0

    def test_direct_action_call_no_policies(self) -> None:
        """Test: await action() - direct call without run_action wrapper."""

        # CrashRecoveryWorkflow uses run_action, so all have policies
        # Let's check a different workflow
        from tests.fixtures_policy.integration_exception_custom import ExceptionCustomWorkflow

        program = ExceptionCustomWorkflow.workflow_ir()

        # provide_value is called directly (not via run_action)
        action = self._find_action_with_policies(program, "provide_value")
        assert action is not None, "Expected @provide_value action"
        assert len(action.policies) == 0, "Direct action call should have no policies"


class TestAsyncioGatherDetection:
    """Test that asyncio.gather is detected and converted to parallel blocks."""

    def _find_parallel_expr(self, program: ir.Program) -> tuple[ir.ParallelExpr, list[str]] | None:
        """Find the first parallel expression in the program.

        Returns tuple of (ParallelExpr, targets) where targets are the assignment variables.
        """
        for fn in program.functions:
            for stmt in fn.body.statements:
                # Check for parallel block statement (side effect only)
                if stmt.HasField("parallel_block"):
                    parallel = ir.ParallelExpr()
                    parallel.calls.extend(stmt.parallel_block.calls)
                    return (parallel, [])
                # Check for assignment with parallel expression
                if stmt.HasField("assignment"):
                    if stmt.assignment.value.HasField("parallel_expr"):
                        return (
                            stmt.assignment.value.parallel_expr,
                            list(stmt.assignment.targets),
                        )
        return None

    def _find_all_parallel_exprs(
        self, program: ir.Program
    ) -> list[tuple[ir.ParallelExpr, list[str]]]:
        """Find all parallel expressions in the program."""
        results: list[tuple[ir.ParallelExpr, list[str]]] = []
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("parallel_block"):
                    parallel = ir.ParallelExpr()
                    parallel.calls.extend(stmt.parallel_block.calls)
                    results.append((parallel, []))
                if stmt.HasField("assignment"):
                    if stmt.assignment.value.HasField("parallel_expr"):
                        results.append(
                            (
                                stmt.assignment.value.parallel_expr,
                                list(stmt.assignment.targets),
                            )
                        )
        return results

    def _get_action_names_from_parallel(
        self, block: ir.ParallelExpr | ir.ParallelBlock
    ) -> list[str]:
        """Extract action names from a parallel block/expr."""
        names = []
        for call in block.calls:
            if call.HasField("action"):
                names.append(call.action.action_name)
        return names

    def test_gather_simple_two_actions(self) -> None:
        """Test: a, b = await asyncio.gather(action_a(), action_b(), return_exceptions=True)"""
        from tests.fixtures_gather.gather_simple import GatherSimpleWorkflow

        program = GatherSimpleWorkflow.workflow_ir()

        result = self._find_parallel_expr(program)
        assert result is not None, "Expected parallel expression from asyncio.gather"
        parallel, targets = result

        # Check targets for tuple unpacking
        assert targets == ["a", "b"], f"Expected targets ['a', 'b'], got {targets}"

        action_names = self._get_action_names_from_parallel(parallel)
        assert len(action_names) == 2, f"Expected 2 actions in parallel, got {len(action_names)}"
        assert "action_a" in action_names, "Expected action_a in parallel block"
        assert "action_b" in action_names, "Expected action_b in parallel block"

    def test_gather_with_args(self) -> None:
        """Test: asyncio.gather with actions that have arguments."""
        from tests.fixtures_gather.gather_with_args import GatherWithArgsWorkflow

        program = GatherWithArgsWorkflow.workflow_ir()

        result = self._find_parallel_expr(program)
        assert result is not None, "Expected parallel expression from asyncio.gather"
        parallel, _targets = result

        action_names = self._get_action_names_from_parallel(parallel)
        assert "compute_square" in action_names, "Expected compute_square in parallel"
        assert "compute_cube" in action_names, "Expected compute_cube in parallel"

        # Check that kwargs are preserved
        for call in parallel.calls:
            if call.HasField("action"):
                action = call.action
                if action.action_name in ("compute_square", "compute_cube"):
                    assert len(action.kwargs) == 1, f"Expected 1 kwarg for {action.action_name}"
                    assert action.kwargs[0].name == "n", "Expected 'n' kwarg"

    def test_gather_to_single_variable(self) -> None:
        """Test: results = await asyncio.gather(a(), b(), c(), return_exceptions=True)"""
        from tests.fixtures_gather.gather_to_variable import GatherToVariableWorkflow

        program = GatherToVariableWorkflow.workflow_ir()

        result = self._find_parallel_expr(program)
        assert result is not None, "Expected parallel expression from asyncio.gather"
        parallel, targets = result

        # Check target variable is set (single target in list)
        assert targets == ["results"], f"Expected targets ['results'], got {targets}"

        action_names = self._get_action_names_from_parallel(parallel)
        assert len(action_names) == 3, f"Expected 3 actions, got {len(action_names)}"

    def test_gather_nested_fan_in(self) -> None:
        """Test: Fan-out with gather, then fan-in with another action."""
        from tests.fixtures_gather.gather_nested import GatherNestedWorkflow

        program = GatherNestedWorkflow.workflow_ir()

        # Should have a parallel expression for the gather
        result = self._find_parallel_expr(program)
        assert result is not None, "Expected parallel expression from asyncio.gather"
        parallel, _targets = result

        action_names = self._get_action_names_from_parallel(parallel)
        assert "fetch_a" in action_names, "Expected fetch_a in parallel"
        assert "fetch_b" in action_names, "Expected fetch_b in parallel"

        # Should also have the combine action after the parallel block
        # Now it's in an assignment with action expression
        combine_found = False
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("assignment"):
                    if stmt.assignment.value.HasField("action_call"):
                        if stmt.assignment.value.action_call.action_name == "combine":
                            combine_found = True
        assert combine_found, "Expected combine action after parallel block"

    def test_gather_starred_list_comprehension(self) -> None:
        """Test: await asyncio.gather(*[action(x) for x in items], return_exceptions=True)

        This pattern is converted to a SpreadExpr in the IR.
        """
        from tests.fixtures_gather.gather_listcomp import GatherListCompWorkflow

        program = GatherListCompWorkflow.workflow_ir()

        # Find the SpreadExpr in the IR
        spread_found = False
        spread_expr = None
        targets = []
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("assignment"):
                    if stmt.assignment.value.HasField("spread_expr"):
                        spread_found = True
                        spread_expr = stmt.assignment.value.spread_expr
                        targets = list(stmt.assignment.targets)

        assert spread_found, "Expected spread expression from asyncio.gather(*[...])"
        assert spread_expr is not None

        # Check the spread details
        assert spread_expr.loop_var == "item"
        assert spread_expr.action.action_name == "process_item"

        # Check the collection is the 'items' variable
        assert spread_expr.collection.HasField("variable")
        assert spread_expr.collection.variable.name == "items"

        # Check the target
        assert targets == ["results"]

    def test_gather_tuple_unpacking(self) -> None:
        """Test: a, b = await asyncio.gather(action1(), action2(), return_exceptions=True)

        This tests tuple unpacking with asyncio.gather where results
        are unpacked into multiple variables.
        """
        from tests.fixtures_gather.gather_tuple_unpack import GatherTupleUnpackWorkflow

        program = GatherTupleUnpackWorkflow.workflow_ir()

        result = self._find_parallel_expr(program)
        assert result is not None, "Expected parallel expression from asyncio.gather"
        parallel, targets = result

        # Check that we have multiple targets for unpacking
        assert targets == [
            "factorial_value",
            "fib_value",
        ], f"Expected targets ['factorial_value', 'fib_value'], got {targets}"

        action_names = self._get_action_names_from_parallel(parallel)
        assert "compute_factorial" in action_names, "Expected compute_factorial in parallel"
        assert "compute_fibonacci" in action_names, "Expected compute_fibonacci in parallel"

        # Should also have the summarize_math action that uses the unpacked values
        summarize_found = False
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("assignment"):
                    if stmt.assignment.value.HasField("action_call"):
                        if stmt.assignment.value.action_call.action_name == "summarize_math":
                            summarize_found = True
                            # Verify the action uses the unpacked variables as kwargs
                            kwargs = {
                                kw.name: kw for kw in stmt.assignment.value.action_call.kwargs
                            }
                            assert "factorial_value" in kwargs, (
                                "summarize_math should use factorial_value kwarg"
                            )
                            assert "fib_value" in kwargs, (
                                "summarize_math should use fib_value kwarg"
                            )
        assert summarize_found, "Expected summarize_math action using unpacked values"


class TestForLoopConversion:
    """Test for loop conversion to IR."""

    def _find_for_loop(self, program: ir.Program) -> ir.ForLoop | None:
        """Find a for loop in the program."""
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("for_loop"):
                    return stmt.for_loop
        return None

    def test_simple_for_loop_structure(self) -> None:
        """Test: Simple for loop has correct structure."""
        from tests.fixtures_control_flow.for_simple import ForSimpleWorkflow

        program = ForSimpleWorkflow.workflow_ir()

        for_loop = self._find_for_loop(program)
        assert for_loop is not None, "Expected for_loop in IR"

        # Check loop variable
        assert "item" in for_loop.loop_vars, "Expected 'item' as loop variable"

        # Check iterable is present
        assert for_loop.HasField("iterable"), "Expected iterable expression"

    def test_for_loop_body_has_call(self) -> None:
        """Test: For loop body contains the action call."""
        from tests.fixtures_control_flow.for_simple import ForSimpleWorkflow

        program = ForSimpleWorkflow.workflow_ir()

        for_loop = self._find_for_loop(program)
        assert for_loop is not None, "Expected for_loop in IR"

        assert for_loop.HasField("block_body"), "Expected block_body in for loop body"
        assert len(for_loop.block_body.statements) >= 1, "Expected statements in for loop body"

    def test_multi_action_for_keeps_block_body(self) -> None:
        """Test: Multi-action for loop body is emitted as a block (no implicit wrapping)."""
        from tests.fixtures_control_flow.for_multi_action import ForMultiActionWorkflow

        program = ForMultiActionWorkflow.workflow_ir()

        for_loop = self._find_for_loop(program)
        assert for_loop is not None, "Expected for_loop in IR"
        assert for_loop.HasField("block_body"), "Expected block_body in for loop body"
        assert len(for_loop.block_body.statements) >= 2, "Expected multi-statement loop body"


class TestWhileLoopConversion:
    """Test while loop conversion to IR."""

    def _find_while_loop(self, program: ir.Program) -> ir.WhileLoop | None:
        """Find a while loop in the program."""
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("while_loop"):
                    return stmt.while_loop
        return None

    def test_simple_while_loop_structure(self) -> None:
        """Test: Simple while loop has correct structure."""
        from tests.fixtures_control_flow.while_simple import WhileSimpleWorkflow

        program = WhileSimpleWorkflow.workflow_ir()

        while_loop = self._find_while_loop(program)
        assert while_loop is not None, "Expected while_loop in IR"
        assert while_loop.HasField("condition"), "Expected condition in while loop"
        assert while_loop.condition.HasField("binary_op"), "Expected binary condition in while loop"
        assert while_loop.HasField("block_body"), "Expected block_body in while loop"


class TestForLoopAccumulatorDetection:
    """Test detection of accumulator patterns in for loops."""

    def _find_for_loop(self, program: ir.Program) -> ir.ForLoop | None:
        """Find the first for loop in the program."""
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("for_loop"):
                    return stmt.for_loop
        return None

    def test_single_list_append_accumulator(self) -> None:
        """Test: list.append() is normalized to explicit assignment."""
        from tests.fixtures_for_loop.for_single_accumulator import ForSingleAccumulatorWorkflow

        program = ForSingleAccumulatorWorkflow.workflow_ir()
        for_loop = self._find_for_loop(program)
        assert for_loop is not None, "Expected for_loop in IR"

        assert for_loop.HasField("block_body"), "Expected block_body in for loop"
        assert any(
            stmt.HasField("assignment") and "results" in stmt.assignment.targets
            for stmt in for_loop.block_body.statements
        ), "Expected an assignment to 'results' in loop body"

    def test_multi_list_append_in_conditionals(self) -> None:
        """Test: Conditional logic in loop bodies is preserved (no implicit wrapping)."""
        from tests.fixtures_for_loop.for_multi_accumulator import ForMultiAccumulatorWorkflow

        program = ForMultiAccumulatorWorkflow.workflow_ir()
        for_loop = self._find_for_loop(program)
        assert for_loop is not None, "Expected for_loop in IR"

        assert for_loop.HasField("block_body"), "Expected block_body in for loop"
        assert any(stmt.HasField("conditional") for stmt in for_loop.block_body.statements), (
            "Expected conditional statement in loop body"
        )

    def test_for_with_append_original_fixture(self) -> None:
        """Test: Original for_with_append fixture works correctly."""
        from tests.fixtures_for_loop.for_with_append import ForWithAppendWorkflow

        program = ForWithAppendWorkflow.workflow_ir()
        for_loop = self._find_for_loop(program)
        assert for_loop is not None, "Expected for_loop in IR"

        assert for_loop.HasField("block_body"), "Expected block_body in for loop"
        assert any(
            stmt.HasField("assignment") and "results" in stmt.assignment.targets
            for stmt in for_loop.block_body.statements
        ), "Expected an assignment to 'results' in loop body"

    def test_loop_variable_not_detected_as_accumulator(self) -> None:
        """Test: Loop body is emitted directly (block-based loops)."""
        from tests.fixtures_for_loop.for_single_accumulator import ForSingleAccumulatorWorkflow

        program = ForSingleAccumulatorWorkflow.workflow_ir()
        for_loop = self._find_for_loop(program)
        assert for_loop is not None, "Expected for_loop in IR"

        assert for_loop.HasField("block_body"), "Expected block_body-based loops"

    def test_in_scope_variable_not_detected_as_accumulator(self) -> None:
        """Test: Variables defined in loop body stay in the block."""
        from tests.fixtures_for_loop.for_single_accumulator import ForSingleAccumulatorWorkflow

        program = ForSingleAccumulatorWorkflow.workflow_ir()
        for_loop = self._find_for_loop(program)
        assert for_loop is not None, "Expected for_loop in IR"

        assert for_loop.HasField("block_body"), "Expected block_body in for loop"

    def test_accumulator_with_wrapped_body(self) -> None:
        """Test: Accumulators work without implicit wrapper functions."""
        from tests.fixtures_for_loop.for_with_append import ForWithAppendWorkflow

        program = ForWithAppendWorkflow.workflow_ir()
        for_loop = self._find_for_loop(program)
        assert for_loop is not None, "Expected for_loop in IR"

        assert not any(fn.name.startswith("__for_body") for fn in program.functions), (
            "Did not expect implicit __for_body functions"
        )
        assert for_loop.HasField("block_body"), "Expected block_body in for loop"
        assert any(
            stmt.HasField("assignment") and "results" in stmt.assignment.targets
            for stmt in for_loop.block_body.statements
        ), "Expected an assignment to 'results' in loop body"


class TestConditionalAccumulatorDetection:
    """Test detection of accumulator patterns in conditionals."""

    def test_if_with_multi_action_body_and_accumulator(self) -> None:
        """Test: Conditional branch bodies are blocks (no implicit wrapping)."""
        from tests.fixtures_control_flow.if_with_accumulator import IfWithAccumulatorWorkflow

        program = IfWithAccumulatorWorkflow.workflow_ir()
        conditional = None
        for stmt in iter_all_statements(program):
            if stmt.HasField("conditional"):
                conditional = stmt.conditional
                break
        assert conditional is not None, "Expected conditional in IR"
        assert conditional.if_branch.HasField("block_body"), "Expected block_body in if branch"

        assert any(stmt.HasField("return_stmt") for stmt in iter_all_statements(program)), (
            "Expected at least one return statement in workflow IR"
        )


class TestConditionalConversion:
    """Test if/elif/else conversion to IR."""

    def _find_conditional(self, program: ir.Program) -> ir.Conditional | None:
        """Find a conditional in the program."""
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("conditional"):
                    return stmt.conditional
        return None

    def test_simple_if_else_structure(self) -> None:
        """Test: Simple if/else has correct structure."""
        from tests.fixtures_control_flow.if_simple import IfSimpleWorkflow

        program = IfSimpleWorkflow.workflow_ir()

        conditional = self._find_conditional(program)
        assert conditional is not None, "Expected conditional in IR"

        # Should have if_branch with condition
        assert conditional.HasField("if_branch"), "Expected if_branch"
        assert conditional.if_branch.HasField("condition"), "Expected condition expression"
        assert conditional.if_branch.HasField("block_body"), "Expected if_branch block body"

        # Should have else_branch
        assert conditional.HasField("else_branch"), "Expected else_branch"
        assert conditional.else_branch.HasField("block_body"), "Expected else_branch block body"

    def test_elif_chain_creates_branches(self) -> None:
        """Test: if/elif/elif/else creates proper branch structure."""
        from tests.fixtures_control_flow.if_elif_else import IfElifElseWorkflow

        program = IfElifElseWorkflow.workflow_ir()

        conditional = self._find_conditional(program)
        assert conditional is not None, "Expected conditional in IR"

        # Should have elif branches
        assert len(conditional.elif_branches) >= 2, "Expected at least 2 elif branches"

    def test_multi_action_branches_keep_block_bodies(self) -> None:
        """Test: Multi-action if/else branches are emitted as blocks (no implicit wrapping)."""
        from tests.fixtures_control_flow.if_multi_action import IfMultiActionWorkflow

        program = IfMultiActionWorkflow.workflow_ir()

        conditional = self._find_conditional(program)
        assert conditional is not None, "Expected conditional in IR"
        assert conditional.if_branch.HasField("block_body"), "Expected if_branch block body"
        assert len(conditional.if_branch.block_body.statements) >= 2, (
            "Expected multi-statement if branch"
        )
        assert conditional.else_branch.HasField("block_body"), "Expected else_branch block body"
        assert len(conditional.else_branch.block_body.statements) >= 2, (
            "Expected multi-statement else branch"
        )
        assert not any(fn.name.startswith("__if_") for fn in program.functions), (
            "Did not expect implicit __if_* functions"
        )


class TestConditionalWithoutElse:
    """Test if statements without else clause where execution continues after."""

    def _find_conditional(self, program: ir.Program) -> ir.Conditional | None:
        """Find a conditional in the program."""
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("conditional"):
                    return stmt.conditional
        return None

    def _find_all_statements(self, program: ir.Program) -> list[ir.Statement]:
        """Find all statements in the main function."""
        for fn in program.functions:
            if fn.name == "main":
                return list(fn.body.statements)
        return []

    def test_if_without_else_has_continuation(self) -> None:
        """Test: if without else should have statements after it.

        This is the bug case from production: when an if statement has no else,
        and the if condition is false, the workflow should continue to the next
        statement, not get stuck in a dead-end.
        """
        from tests.fixtures_control_flow.if_no_else_continue import IfNoElseContinueWorkflow

        program = IfNoElseContinueWorkflow.workflow_ir()

        conditional = self._find_conditional(program)
        assert conditional is not None, "Expected conditional in IR"

        # Should have if_branch but NO else_branch
        assert conditional.HasField("if_branch"), "Expected if_branch"
        assert not conditional.HasField("else_branch") or not conditional.else_branch.HasField(
            "block_body"
        ), "Should not have else_branch (or it should be empty)"

        # The main function should have statements AFTER the conditional
        # (the finalize action call)
        statements = self._find_all_statements(program)
        assert len(statements) > 0, "Should have statements in main function"

        # Find the conditional's position
        conditional_idx = None
        for i, stmt in enumerate(statements):
            if stmt.HasField("conditional"):
                conditional_idx = i
                break

        assert conditional_idx is not None, "Should find conditional in statements"

        # There should be at least one statement after the conditional
        # (the finalize action and return)
        statements_after = statements[conditional_idx + 1 :]
        assert len(statements_after) >= 1, (
            f"Expected statements after conditional, but found none. "
            f"Total statements: {len(statements)}, conditional at index: {conditional_idx}"
        )


class TestTryExceptConversion:
    """Test try/except conversion to IR."""

    def _find_try_except(self, program: ir.Program) -> ir.TryExcept | None:
        """Find a try/except in the program."""
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("try_except"):
                    return stmt.try_except
        return None

    def test_simple_try_except_structure(self) -> None:
        """Test: Simple try/except has correct structure."""
        from tests.fixtures_control_flow.try_simple import TrySimpleWorkflow

        program = TrySimpleWorkflow.workflow_ir()

        try_except = self._find_try_except(program)
        assert try_except is not None, "Expected try_except in IR"

        # Should have try body
        assert try_except.HasField("try_block"), "Expected try_block"

        # Should have at least one handler
        assert len(try_except.handlers) >= 1, "Expected at least one exception handler"

    def test_multi_action_try_keeps_block_body(self) -> None:
        """Test: Multi-action try body is emitted as a block (no implicit wrapping)."""
        from tests.fixtures_control_flow.try_multi_action import TryMultiActionWorkflow

        program = TryMultiActionWorkflow.workflow_ir()

        try_except = self._find_try_except(program)
        assert try_except is not None, "Expected try_except in IR"
        assert try_except.HasField("try_block"), "Expected try_block"
        assert len(try_except.try_block.statements) >= 2, "Expected multi-statement try body"
        assert not any(fn.name.startswith("__try_body") for fn in program.functions), (
            "Did not expect implicit __try_body* functions"
        )

    def test_multiple_exception_handlers(self) -> None:
        """Test: Multiple except clauses create multiple handlers."""
        from tests.fixtures_control_flow.try_multi_except import TryMultiExceptWorkflow

        program = TryMultiExceptWorkflow.workflow_ir()

        try_except = self._find_try_except(program)
        assert try_except is not None, "Expected try_except in IR"

        # Should have multiple handlers
        assert len(try_except.handlers) >= 3, (
            f"Expected at least 3 exception handlers, got {len(try_except.handlers)}"
        )

        # Check exception types are captured (exception_types is a repeated field)
        all_exception_types: List[str] = []
        for h in try_except.handlers:
            all_exception_types.extend(h.exception_types)
        assert "ValueError" in all_exception_types, "Expected ValueError handler"
        assert "TypeError" in all_exception_types, "Expected TypeError handler"

    def test_exception_handler_captures_variable(self) -> None:
        """Test: except ... as var captures exception variable."""
        from tests.fixtures_control_flow.try_except_capture import TryExceptCaptureWorkflow

        program = TryExceptCaptureWorkflow.workflow_ir()

        try_except = self._find_try_except(program)
        assert try_except is not None, "Expected try_except in IR"
        assert len(try_except.handlers) == 1, "Expected single exception handler"
        handler = try_except.handlers[0]
        assert handler.exception_var == "err"


class TestActionCallExtraction:
    """Test action call detection and argument handling."""

    def _find_action_call(
        self, program: ir.Program, name: str
    ) -> tuple[ir.ActionCall, list[str]] | None:
        """Find an action call by name.

        Returns tuple of (ActionCall, targets) where targets are assignment variables.
        """
        for fn in program.functions:
            for stmt in fn.body.statements:
                # Check for side-effect only action statement
                if stmt.HasField("action_call"):
                    if stmt.action_call.action_name == name:
                        return (stmt.action_call, [])
                # Check for assignment with action expression
                if stmt.HasField("assignment"):
                    if stmt.assignment.value.HasField("action_call"):
                        if stmt.assignment.value.action_call.action_name == name:
                            return (
                                stmt.assignment.value.action_call,
                                list(stmt.assignment.targets),
                            )
        return None

    def _find_all_action_calls(self, program: ir.Program) -> list[tuple[ir.ActionCall, list[str]]]:
        """Find all action calls in the program.

        Returns list of (ActionCall, targets) tuples.
        """
        calls: list[tuple[ir.ActionCall, list[str]]] = []
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("action_call"):
                    calls.append((stmt.action_call, []))
                if stmt.HasField("assignment"):
                    if stmt.assignment.value.HasField("action_call"):
                        calls.append(
                            (
                                stmt.assignment.value.action_call,
                                list(stmt.assignment.targets),
                            )
                        )
        return calls

    def test_action_with_kwargs(self) -> None:
        """Test: Action called with keyword arguments preserves kwargs."""
        from tests.fixtures_actions.action_kwargs import ActionKwargsWorkflow

        program = ActionKwargsWorkflow.workflow_ir()

        result = self._find_action_call(program, "greet_person")
        assert result is not None, "Expected greet_person action"
        action, _targets = result

        # Check kwargs
        kwarg_names = [kw.name for kw in action.kwargs]
        assert "name" in kwarg_names, "Expected 'name' kwarg"
        assert "greeting" in kwarg_names, "Expected 'greeting' kwarg"

    def test_action_with_positional_args_converted_to_kwargs(self) -> None:
        """Test: Positional arguments are converted to kwargs using signature.

        The IR builder converts positional args to kwargs using the
        action's signature. This requires the action to be properly decorated.
        """
        from tests.fixtures_actions.action_positional_args import ActionPositionalArgsWorkflow

        program = ActionPositionalArgsWorkflow.workflow_ir()

        result = self._find_action_call(program, "add_numbers")
        assert result is not None, "Expected add_numbers action"
        action, _targets = result

        # The IR builder should have 2 kwargs (from positional args)
        # They get converted using the action's signature
        assert len(action.kwargs) == 2, (
            f"Expected 2 kwargs from positional args, got {len(action.kwargs)}"
        )

        # Verify parameter names match the action signature (a, b)
        kwarg_names = [kw.name for kw in action.kwargs]
        assert "a" in kwarg_names, "Expected 'a' kwarg from signature"
        assert "b" in kwarg_names, "Expected 'b' kwarg from signature"

        # Verify the values are literals (10 and 20) with correct mapping
        for kw in action.kwargs:
            if kw.name == "a":
                assert kw.value.HasField("literal"), "Expected literal for 'a'"
                assert kw.value.literal.int_value == 10, "Expected a=10"
            elif kw.name == "b":
                assert kw.value.HasField("literal"), "Expected literal for 'b'"
                assert kw.value.literal.int_value == 20, "Expected b=20"

    def test_action_with_variable_references(self) -> None:
        """Test: Action arguments that are variable references."""
        from tests.fixtures_actions.action_variable_args import ActionVariableArgsWorkflow

        program = ActionVariableArgsWorkflow.workflow_ir()

        result = self._find_action_call(program, "multiply_by")
        assert result is not None, "Expected multiply_by action"
        action, _targets = result

        # Check that kwargs reference variables
        for kw in action.kwargs:
            if kw.name == "value":
                assert kw.value.HasField("variable"), "Expected variable reference for 'value'"
                assert kw.value.variable.name == "base", "Expected reference to 'base'"
            elif kw.name == "factor":
                assert kw.value.HasField("variable"), "Expected variable reference for 'factor'"
                assert kw.value.variable.name == "factor", "Expected reference to 'factor'"

    def test_action_without_assignment(self) -> None:
        """Test: Action called without capturing return value."""
        from tests.fixtures_actions.action_no_assignment import ActionNoAssignmentWorkflow

        program = ActionNoAssignmentWorkflow.workflow_ir()

        # Find log_event calls - should exist without target
        calls = self._find_all_action_calls(program)
        log_calls = [(c, t) for c, t in calls if c.action_name == "log_event"]

        assert len(log_calls) >= 1, "Expected at least one log_event call"

        # At least one should have no target (side effect only = empty targets list)
        has_no_target = any(len(targets) == 0 for _call, targets in log_calls)
        assert has_no_target, "Expected log_event call without target assignment"

    def test_action_target_variable_captured(self) -> None:
        """Test: Action result is assigned to correct target variable."""
        from tests.fixtures_actions.action_kwargs import ActionKwargsWorkflow

        program = ActionKwargsWorkflow.workflow_ir()

        result = self._find_action_call(program, "greet_person")
        assert result is not None, "Expected greet_person action"
        _action, targets = result

        # Should have target
        assert targets == ["result"], f"Expected targets ['result'], got {targets}"

    def test_action_module_name_set(self) -> None:
        """Test: Action has module_name set for worker dispatch."""
        from tests.fixtures_actions.action_kwargs import ActionKwargsWorkflow

        program = ActionKwargsWorkflow.workflow_ir()

        result = self._find_action_call(program, "greet_person")
        assert result is not None, "Expected greet_person action"
        action, _targets = result

        # Should have module name
        assert action.module_name, "Expected module_name to be set"
        assert "action_kwargs" in action.module_name, (
            f"Expected module name to contain 'action_kwargs', got '{action.module_name}'"
        )

    def test_action_with_mixed_positional_and_keyword_args(self) -> None:
        """Test: Mix of positional and keyword args are all converted to kwargs."""
        from tests.fixtures_actions.action_mixed_args import ActionMixedArgsWorkflow

        program = ActionMixedArgsWorkflow.workflow_ir()

        result = self._find_action_call(program, "compute_value")
        assert result is not None, "Expected compute_value action"
        action, _targets = result

        # Should have 3 kwargs (2 from positional, 1 explicit kwarg)
        assert len(action.kwargs) == 3, f"Expected 3 kwargs, got {len(action.kwargs)}"

        # Verify all parameter names are present
        kwarg_names = [kw.name for kw in action.kwargs]
        assert "x" in kwarg_names, "Expected 'x' kwarg from positional arg"
        assert "y" in kwarg_names, "Expected 'y' kwarg from positional arg"
        assert "multiplier" in kwarg_names, "Expected 'multiplier' kwarg"

        # Verify values are correct
        for kw in action.kwargs:
            if kw.name == "x":
                assert kw.value.literal.int_value == 5, "Expected x=5"
            elif kw.name == "y":
                assert kw.value.literal.int_value == 10, "Expected y=10"
            elif kw.name == "multiplier":
                assert kw.value.literal.int_value == 2, "Expected multiplier=2"


class TestWorkflowHelperMethods:
    """Test that self.method() calls are converted to FunctionCall IR nodes."""

    def _find_function_call_in_assignments(
        self, program: ir.Program, func_name: str
    ) -> Optional[ir.FunctionCall]:
        """Find a function call by name in assignment statements."""
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("assignment"):
                    if stmt.assignment.value.HasField("function_call"):
                        fc = stmt.assignment.value.function_call
                        if fc.name == func_name:
                            return fc
        return None

    def test_helper_method_converted_to_function_call(self) -> None:
        """Test: self.method() calls become FunctionCall nodes."""
        from tests.fixtures_workflow.workflow_helper_methods import WorkflowWithHelperMethods

        program = WorkflowWithHelperMethods.workflow_ir()

        # Should find compute_multiplier as a function call
        fc = self._find_function_call_in_assignments(program, "compute_multiplier")
        assert fc is not None, "Expected compute_multiplier as FunctionCall"

        # Should find format_result as a function call
        fc2 = self._find_function_call_in_assignments(program, "format_result")
        assert fc2 is not None, "Expected format_result as FunctionCall"

    def test_helper_method_kwargs_preserved(self) -> None:
        """Test: self.method(a=x, b=y) preserves keyword arguments."""
        from tests.fixtures_workflow.workflow_helper_methods import WorkflowWithHelperMethods

        program = WorkflowWithHelperMethods.workflow_ir()

        fc = self._find_function_call_in_assignments(program, "compute_multiplier")
        assert fc is not None, "Expected compute_multiplier"

        # Check kwargs are preserved
        kwarg_names = [kw.name for kw in fc.kwargs]
        assert "base" in kwarg_names, "Expected 'base' kwarg"
        assert "factor" in kwarg_names, "Expected 'factor' kwarg"

        # Verify values are variable references to the input parameters
        for kw in fc.kwargs:
            if kw.name == "base":
                assert kw.value.HasField("variable"), "Expected variable reference"
                assert kw.value.variable.name == "base", "Expected reference to 'base'"
            elif kw.name == "factor":
                assert kw.value.HasField("variable"), "Expected variable reference"
                assert kw.value.variable.name == "factor", "Expected reference to 'factor'"

    def test_helper_method_with_variable_arg(self) -> None:
        """Test: self.method(value=some_var) passes variable references correctly."""
        from tests.fixtures_workflow.workflow_helper_methods import WorkflowWithHelperMethods

        program = WorkflowWithHelperMethods.workflow_ir()

        fc = self._find_function_call_in_assignments(program, "format_result")
        assert fc is not None, "Expected format_result"

        # Check the 'value' kwarg references 'processed' variable
        assert len(fc.kwargs) == 1, f"Expected 1 kwarg, got {len(fc.kwargs)}"
        assert fc.kwargs[0].name == "value", "Expected 'value' kwarg"
        assert fc.kwargs[0].value.HasField("variable"), "Expected variable reference"
        assert fc.kwargs[0].value.variable.name == "processed", (
            f"Expected reference to 'processed', got '{fc.kwargs[0].value.variable.name}'"
        )

    def test_helper_method_with_positional_args(self) -> None:
        """Test: self.method(a, b) preserves positional args in fc.args."""
        from tests.fixtures_workflow.workflow_helper_positional import WorkflowHelperPositionalArgs

        program = WorkflowHelperPositionalArgs.workflow_ir()

        fc = self._find_function_call_in_assignments(program, "add")
        assert fc is not None, "Expected add"

        # Positional args should be in fc.args (not converted to kwargs)
        assert len(fc.args) == 2, f"Expected 2 positional args, got {len(fc.args)}"
        assert len(fc.kwargs) == 0, f"Expected 0 kwargs, got {len(fc.kwargs)}"

        # First arg should be variable reference to 'value'
        assert fc.args[0].HasField("variable"), "Expected first arg to be variable"
        assert fc.args[0].variable.name == "value", "Expected reference to 'value'"

        # Second arg should be literal 10
        assert fc.args[1].HasField("literal"), "Expected second arg to be literal"
        assert fc.args[1].literal.int_value == 10, "Expected literal 10"

    def test_helper_method_with_mixed_args(self) -> None:
        """Test: self.method(a, b, c=x) preserves both positional and keyword args."""
        from tests.fixtures_workflow.workflow_helper_positional import WorkflowHelperPositionalArgs

        program = WorkflowHelperPositionalArgs.workflow_ir()

        fc = self._find_function_call_in_assignments(program, "multiply")
        assert fc is not None, "Expected multiply"

        # Should have 2 positional args and 1 kwarg
        assert len(fc.args) == 2, f"Expected 2 positional args, got {len(fc.args)}"
        assert len(fc.kwargs) == 1, f"Expected 1 kwarg, got {len(fc.kwargs)}"

        # Positional args
        assert fc.args[0].variable.name == "sum_result", "Expected reference to 'sum_result'"
        assert fc.args[1].literal.int_value == 2, "Expected literal 2"

        # Keyword arg
        assert fc.kwargs[0].name == "z", "Expected 'z' kwarg"
        assert fc.kwargs[0].value.literal.int_value == 3, "Expected z=3"

    def test_async_helper_method_included(self) -> None:
        """Test: await self.method() becomes a FunctionCall with a definition."""
        from tests.fixtures_workflow.workflow_helper_async import WorkflowWithAsyncHelper

        program = WorkflowWithAsyncHelper.workflow_ir()

        fc = self._find_function_call_in_assignments(program, "run_internal")
        assert fc is not None, "Expected run_internal as FunctionCall"

        fn_names = {fn.name for fn in program.functions}
        assert "run_internal" in fn_names, "Expected run_internal function definition"

    def test_helper_method_from_base_class(self) -> None:
        """Test: base-class helper methods are included in the IR."""
        from tests.fixtures_workflow.workflow_helper_inheritance_child import (
            WorkflowWithHelperInheritance,
        )

        program = WorkflowWithHelperInheritance.workflow_ir()

        fc = self._find_function_call_in_assignments(program, "run_internal")
        assert fc is not None, "Expected run_internal as FunctionCall"

        fn_names = {fn.name for fn in program.functions}
        assert "run_internal" in fn_names, "Expected run_internal function definition"

        assert fc.kwargs[0].name == "value", "Expected 'value' kwarg"

    def test_helper_method_default_args_filled_in(self) -> None:
        """Test: self.method(required_arg=x) fills in defaults for omitted optional args."""
        from tests.fixtures_workflow.workflow_default_args import WorkflowWithDefaultArgs

        program = WorkflowWithDefaultArgs.workflow_ir()

        fc = self._find_function_call_in_assignments(program, "helper_with_defaults")
        assert fc is not None, "Expected helper_with_defaults as FunctionCall"

        # Should have 3 kwargs: required_arg (provided) + optional_arg (default) + optional_str (default)
        kwarg_names = {kw.name for kw in fc.kwargs}
        assert "required_arg" in kwarg_names, "Expected 'required_arg' kwarg"
        assert "optional_arg" in kwarg_names, "Expected 'optional_arg' kwarg (default filled in)"
        assert "optional_str" in kwarg_names, "Expected 'optional_str' kwarg (default filled in)"

        # Verify default values
        for kw in fc.kwargs:
            if kw.name == "required_arg":
                assert kw.value.HasField("variable"), "Expected variable reference for required_arg"
                assert kw.value.variable.name == "value", "Expected reference to 'value'"
            elif kw.name == "optional_arg":
                assert kw.value.HasField("literal"), "Expected literal for optional_arg"
                assert kw.value.literal.is_none, "Expected None literal for optional_arg"
            elif kw.name == "optional_str":
                assert kw.value.HasField("literal"), "Expected literal for optional_str"
                assert kw.value.literal.string_value == "default", "Expected 'default' string"

    def test_helper_method_int_default_filled_in(self) -> None:
        """Test: int default values are filled in correctly."""
        from tests.fixtures_workflow.workflow_default_args import WorkflowWithDefaultArgs

        program = WorkflowWithDefaultArgs.workflow_ir()

        fc = self._find_function_call_in_assignments(program, "helper_with_int_default")
        assert fc is not None, "Expected helper_with_int_default as FunctionCall"

        kwarg_names = {kw.name for kw in fc.kwargs}
        assert "value" in kwarg_names, "Expected 'value' kwarg"
        assert "multiplier" in kwarg_names, "Expected 'multiplier' kwarg (default filled in)"

        for kw in fc.kwargs:
            if kw.name == "multiplier":
                assert kw.value.HasField("literal"), "Expected literal for multiplier"
                assert kw.value.literal.int_value == 10, "Expected multiplier=10"

    def test_helper_method_chained_calls_have_defaults(self) -> None:
        """Test: Multiple function calls in sequence all have defaults filled in."""
        from tests.fixtures_workflow.workflow_default_args import WorkflowWithNestedCalls

        program = WorkflowWithNestedCalls.workflow_ir()

        # get_multiplier() should have base=2 filled in
        fc = self._find_function_call_in_assignments(program, "get_multiplier")
        assert fc is not None, "Expected get_multiplier as FunctionCall"

        kwarg_names = {kw.name for kw in fc.kwargs}
        assert "base" in kwarg_names, "Expected 'base' kwarg (default filled in)"

        for kw in fc.kwargs:
            if kw.name == "base":
                assert kw.value.HasField("literal"), "Expected literal for base"
                assert kw.value.literal.int_value == 2, "Expected base=2"

    def test_truly_nested_function_call_defaults(self) -> None:
        """Test: Function calls as arguments to other calls have defaults filled in."""
        from tests.fixtures_workflow.workflow_default_args import WorkflowWithTrulyNestedCalls

        program = WorkflowWithTrulyNestedCalls.workflow_ir()

        # Find the apply_offset call
        fc = self._find_function_call_in_assignments(program, "apply_offset")
        assert fc is not None, "Expected apply_offset as FunctionCall"

        # Check that offset kwarg contains a nested function call
        offset_kwarg = None
        for kw in fc.kwargs:
            if kw.name == "offset":
                offset_kwarg = kw
                break
        assert offset_kwarg is not None, "Expected 'offset' kwarg"
        assert offset_kwarg.value.HasField("function_call"), "Expected nested function call"

        # The nested get_offset() call should have delta=5 filled in
        nested_fc = offset_kwarg.value.function_call
        assert nested_fc.name == "get_offset", "Expected get_offset call"

        kwarg_names = {kw.name for kw in nested_fc.kwargs}
        assert "delta" in kwarg_names, "Expected 'delta' kwarg (default filled in)"

        for kw in nested_fc.kwargs:
            if kw.name == "delta":
                assert kw.value.HasField("literal"), "Expected literal for delta"
                assert kw.value.literal.int_value == 5, "Expected delta=5"


class TestUnsupportedPatternDetection:
    """Test that unsupported patterns raise UnsupportedPatternError with recommendations."""

    def test_gather_variable_spread_raises_error(self) -> None:
        """Test: asyncio.gather(*tasks, return_exceptions=True) raises error."""
        import pytest

        from tests.fixtures_gather.gather_unsupported_variable import (
            GatherUnsupportedVariableWorkflow,
        )
        from waymark import UnsupportedPatternError

        with pytest.raises(UnsupportedPatternError) as exc_info:
            GatherUnsupportedVariableWorkflow.workflow_ir()

        error = exc_info.value
        assert isinstance(error, UnsupportedPatternError)
        assert "tasks" in error.message, "Error should mention the variable name"
        assert "gather" in error.message.lower(), "Error should mention gather"
        assert "list comprehension" in error.recommendation.lower(), (
            "Recommendation should suggest list comprehension"
        )

    def test_gather_requires_return_exceptions(self) -> None:
        """Test: asyncio.gather without return_exceptions raises error."""
        import asyncio

        import pytest

        from waymark import UnsupportedPatternError, action, workflow
        from waymark.workflow import Workflow

        @action
        async def action_a() -> int:
            return 1

        @action
        async def action_b() -> int:
            return 2

        @workflow
        class GatherMissingReturnExceptionsWorkflow(Workflow):
            async def run(self) -> tuple:
                results = await asyncio.gather(action_a(), action_b())
                return results

        with pytest.raises(UnsupportedPatternError) as exc_info:
            GatherMissingReturnExceptionsWorkflow.workflow_ir()

        error = exc_info.value
        assert isinstance(error, UnsupportedPatternError)
        assert "return_exceptions" in error.message, (
            "Error should mention return_exceptions requirement"
        )

    def test_fstring_raises_error(self) -> None:
        """Test: f-strings raise error with recommendation."""
        import pytest

        from waymark import UnsupportedPatternError, action, workflow
        from waymark.workflow import Workflow

        @action(name="fstring_test_action")
        async def fstring_action() -> int:
            return 1

        @workflow
        class FstringWorkflow(Workflow):
            async def run(self, name: str) -> str:
                result = await fstring_action()
                return f"Hello {name}, result is {result}"

        with pytest.raises(UnsupportedPatternError) as exc_info:
            FstringWorkflow.workflow_ir()

        error = exc_info.value
        assert isinstance(error, UnsupportedPatternError)
        assert "F-string" in error.message, "Error should mention f-strings"
        assert "@action" in error.recommendation, "Recommendation should suggest using @action"

    def test_with_statement_raises_error(self) -> None:
        """Test: with statements raise error with recommendation."""
        import pytest

        from waymark import UnsupportedPatternError, workflow
        from waymark.workflow import Workflow

        @workflow
        class WithWorkflow(Workflow):
            async def run(self, path: str) -> str:
                with open(path) as f:
                    return f.read()

        with pytest.raises(UnsupportedPatternError) as exc_info:
            WithWorkflow.workflow_ir()

        error = exc_info.value
        assert isinstance(error, UnsupportedPatternError)
        assert "with" in error.message.lower(), "Error should mention with statements"
        assert "@action" in error.recommendation, "Recommendation should suggest using @action"

    def test_lambda_raises_error(self) -> None:
        """Test: lambda expressions raise error with recommendation."""
        import pytest

        from waymark import UnsupportedPatternError, workflow
        from waymark.workflow import Workflow

        @workflow
        class LambdaWorkflow(Workflow):
            async def run(self, x: int) -> int:
                fn = lambda y: y * 2  # noqa: E731
                return fn(x)

        with pytest.raises(UnsupportedPatternError) as exc_info:
            LambdaWorkflow.workflow_ir()

        error = exc_info.value
        assert isinstance(error, UnsupportedPatternError)
        assert "Lambda" in error.message, "Error should mention lambda"
        assert "@action" in error.recommendation, "Recommendation should suggest using @action"

    def test_list_comprehension_assignment_is_supported(self) -> None:
        """Test: list comprehension assignments expand into for loop IR."""
        from tests.fixtures_comprehension.list_comprehension_assignment import (
            ListComprehensionAssignmentWorkflow,
        )

        program = ListComprehensionAssignmentWorkflow.workflow_ir()

        run_fn = next(fn for fn in program.functions if fn.name == "main")

        has_initializer = any(
            stmt.HasField("assignment")
            and list(stmt.assignment.targets) == ["active_users"]
            and stmt.assignment.value.HasField("list")
            and len(stmt.assignment.value.list.elements) == 0
            for stmt in run_fn.body.statements
        )
        assert has_initializer, "Expected accumulator initialization for active_users"

        for_loop = next(
            (stmt.for_loop for stmt in run_fn.body.statements if stmt.HasField("for_loop")),
            None,
        )
        assert for_loop is not None, "Expected for loop generated from list comprehension"
        assert "user" in for_loop.loop_vars, "Loop variable should match comprehension"
        assert for_loop.HasField("block_body"), "Expected block_body in for loop"
        assert any(
            stmt.HasField("assignment") and "active_users" in stmt.assignment.targets
            for stmt in iter_block_statements(for_loop.block_body)
        ), "Expected accumulator updates for active_users in loop body"

    def test_list_comprehension_ifexp_assignment_is_supported(self) -> None:
        """Test: ternary expressions in list comprehensions expand into conditional IR."""
        from tests.fixtures_comprehension.list_comprehension_ifexp import (
            ListComprehensionIfExpWorkflow,
        )

        program = ListComprehensionIfExpWorkflow.workflow_ir()

        for_loop = next(
            (
                stmt.for_loop
                for fn in program.functions
                for stmt in fn.body.statements
                if stmt.HasField("for_loop")
            ),
            None,
        )
        assert for_loop is not None, "Expected for loop generated from list comprehension"
        assert for_loop.HasField("block_body"), "Expected block_body in for loop"
        assert any(stmt.HasField("conditional") for stmt in for_loop.block_body.statements), (
            "Expected conditional derived from ternary expression in loop body"
        )
        assert any(
            stmt.HasField("assignment") and "statuses" in stmt.assignment.targets
            for stmt in iter_all_statements(program)
        ), "Expected accumulator updates for statuses"

    def test_dict_comprehension_assignment_is_supported(self) -> None:
        """Test: dict comprehension assignments expand into for loop IR."""
        from tests.fixtures_comprehension.dict_comprehension_assignment import (
            DictComprehensionAssignmentWorkflow,
        )

        program = DictComprehensionAssignmentWorkflow.workflow_ir()

        run_fn = next(fn for fn in program.functions if fn.name == "main")
        has_initializer = any(
            stmt.HasField("assignment")
            and list(stmt.assignment.targets) == ["active_lookup"]
            and stmt.assignment.value.HasField("dict")
            and len(stmt.assignment.value.dict.entries) == 0
            for stmt in run_fn.body.statements
        )
        assert has_initializer, "Expected accumulator initialization for active_lookup"

        for_loop = next(
            (stmt.for_loop for stmt in run_fn.body.statements if stmt.HasField("for_loop")),
            None,
        )
        assert for_loop is not None, "Expected for loop generated from dict comprehension"
        assert "user" in for_loop.loop_vars
        assert for_loop.HasField("block_body"), "Expected block_body in for loop"
        assert any(
            stmt.HasField("assignment")
            and any(target.startswith("active_lookup[") for target in stmt.assignment.targets)
            for stmt in iter_block_statements(for_loop.block_body)
        ), "Expected accumulator updates for active_lookup in loop body"

    def test_dict_comprehension_tuple_assignment_is_supported(self) -> None:
        """Test: dict comprehension assignments with tuple unpacking expand to IR."""
        from tests.fixtures_comprehension.dict_comprehension_tuple import (
            DictComprehensionTupleWorkflow,
        )

        program = DictComprehensionTupleWorkflow.workflow_ir()
        for_loop = next(
            (
                stmt.for_loop
                for fn in program.functions
                for stmt in fn.body.statements
                if stmt.HasField("for_loop")
            ),
            None,
        )
        assert for_loop is not None
        assert for_loop.HasField("block_body"), "Expected block_body in for loop"
        assert any(
            stmt.HasField("assignment")
            and any(target.startswith("mapping[") for target in stmt.assignment.targets)
            for stmt in iter_block_statements(for_loop.block_body)
        ), "Expected accumulator updates for mapping in loop body"

    def test_delete_statement_raises_error(self) -> None:
        """Test: del statements raise error with recommendation."""
        import pytest

        from waymark import UnsupportedPatternError, workflow
        from waymark.workflow import Workflow

        @workflow
        class DeleteWorkflow(Workflow):
            async def run(self, data: dict) -> dict:
                del data["key"]
                return data

        with pytest.raises(UnsupportedPatternError) as exc_info:
            DeleteWorkflow.workflow_ir()

        error = exc_info.value
        assert isinstance(error, UnsupportedPatternError)
        assert "del" in error.message.lower(), "Error should mention del"
        assert "@action" in error.recommendation, "Recommendation should suggest using @action"

    def test_error_includes_line_number(self) -> None:
        """Test: errors include line number for debugging."""
        import pytest

        from tests.fixtures_gather.gather_unsupported_variable import (
            GatherUnsupportedVariableWorkflow,
        )
        from waymark import UnsupportedPatternError

        with pytest.raises(UnsupportedPatternError) as exc_info:
            GatherUnsupportedVariableWorkflow.workflow_ir()

        error = exc_info.value
        assert isinstance(error, UnsupportedPatternError)
        assert error.line is not None, "Error should include line number"
        assert error.line > 0, "Line number should be positive"

    def test_global_statement_raises_error(self) -> None:
        """Test: global statements raise error."""
        import pytest

        from waymark import UnsupportedPatternError, action, workflow
        from waymark.workflow import Workflow

        @action(name="global_test_action")
        async def global_action() -> int:
            return 1

        @workflow
        class GlobalWorkflow(Workflow):
            async def run(self) -> int:
                global some_var  # noqa: PLW0604
                some_var = await global_action()
                return some_var

        with pytest.raises(UnsupportedPatternError) as exc_info:
            GlobalWorkflow.workflow_ir()

        error = exc_info.value
        assert isinstance(error, UnsupportedPatternError)
        assert "Global" in error.message, "Error should mention global"

    def test_nonlocal_statement_raises_error(self) -> None:
        """Test: nonlocal statements raise error."""
        import pytest

        from waymark import UnsupportedPatternError, workflow
        from waymark.workflow import Workflow

        @workflow
        class NonlocalWorkflow(Workflow):
            async def run(self) -> int:
                x = 1

                def inner():
                    nonlocal x
                    x = 2

                inner()
                return x

        # The nested function def will be caught first
        with pytest.raises(UnsupportedPatternError) as exc_info:
            NonlocalWorkflow.workflow_ir()

        error = exc_info.value
        assert isinstance(error, UnsupportedPatternError)
        assert "function" in error.message.lower(), "Error should mention nested function"

    def test_import_inside_run_raises_error(self) -> None:
        """Test: import statements inside run() raise error."""
        import pytest

        from waymark import UnsupportedPatternError, workflow
        from waymark.workflow import Workflow

        @workflow
        class ImportWorkflow(Workflow):
            async def run(self) -> int:
                import json  # noqa: PLC0415

                return len(json.dumps({}))

        with pytest.raises(UnsupportedPatternError) as exc_info:
            ImportWorkflow.workflow_ir()

        error = exc_info.value
        assert isinstance(error, UnsupportedPatternError)
        assert "Import" in error.message, "Error should mention import"

    def test_class_def_inside_run_raises_error(self) -> None:
        """Test: class definitions inside run() raise error."""
        import pytest

        from waymark import UnsupportedPatternError, workflow
        from waymark.workflow import Workflow

        @workflow
        class ClassDefWorkflow(Workflow):
            async def run(self) -> int:
                class Inner:
                    pass

                return 1

        with pytest.raises(UnsupportedPatternError) as exc_info:
            ClassDefWorkflow.workflow_ir()

        error = exc_info.value
        assert isinstance(error, UnsupportedPatternError)
        assert "Class" in error.message, "Error should mention class"

    def test_nested_function_raises_error(self) -> None:
        """Test: nested function definitions raise error."""
        import pytest

        from waymark import UnsupportedPatternError, workflow
        from waymark.workflow import Workflow

        @workflow
        class NestedFuncWorkflow(Workflow):
            async def run(self) -> int:
                def helper():
                    return 1

                return helper()

        with pytest.raises(UnsupportedPatternError) as exc_info:
            NestedFuncWorkflow.workflow_ir()

        error = exc_info.value
        assert isinstance(error, UnsupportedPatternError)
        assert "function" in error.message.lower(), "Error should mention function"


class TestBreakStatementSupport:
    """Test that break statements are properly converted to IR."""

    def test_break_in_for_loop(self) -> None:
        """Test: break statement in a for loop is converted to IR."""
        from tests.fixtures_for_loop.for_break import ForBreakWorkflow

        program = ForBreakWorkflow.workflow_ir()

        # Find the for loop statement
        main_fn = None
        for fn in program.functions:
            if fn.name == "main":
                main_fn = fn
                break

        assert main_fn is not None, "Should have main function"

        # Find the for loop
        for_loop = None
        for stmt in main_fn.body.statements:
            if stmt.HasField("for_loop"):
                for_loop = stmt.for_loop
                break

        assert for_loop is not None, "Should have a for loop"

        # Find the break statement in the loop body
        def find_break_in_block(block: ir.Block) -> bool:
            for stmt in block.statements:
                if stmt.HasField("break_stmt"):
                    return True
                if stmt.HasField("conditional"):
                    cond = stmt.conditional
                    if cond.if_branch.HasField("block_body"):
                        if find_break_in_block(cond.if_branch.block_body):
                            return True
            return False

        has_break = find_break_in_block(for_loop.block_body)
        assert has_break, "Should find break statement in for loop body"


class TestContinueStatementSupport:
    """Test that continue statements are properly converted to IR."""

    def test_continue_in_for_loop(self) -> None:
        """Test: continue statement in a for loop is converted to IR."""
        from tests.fixtures_for_loop.for_continue import ForContinueWorkflow

        program = ForContinueWorkflow.workflow_ir()

        # Find the for loop statement
        main_fn = None
        for fn in program.functions:
            if fn.name == "main":
                main_fn = fn
                break

        assert main_fn is not None, "Should have main function"

        # Find the for loop
        for_loop = None
        for stmt in main_fn.body.statements:
            if stmt.HasField("for_loop"):
                for_loop = stmt.for_loop
                break

        assert for_loop is not None, "Should have a for loop"

        # Find the continue statement in the loop body
        def find_continue_in_block(block: ir.Block) -> bool:
            for stmt in block.statements:
                if stmt.HasField("continue_stmt"):
                    return True
                if stmt.HasField("conditional"):
                    cond = stmt.conditional
                    if cond.if_branch.HasField("block_body"):
                        if find_continue_in_block(cond.if_branch.block_body):
                            return True
            return False

        has_continue = find_continue_in_block(for_loop.block_body)
        assert has_continue, "Should find continue statement in for loop body"


class TestReturnStatements:
    """Test return statement handling."""

    def test_return_with_variable(self) -> None:
        """Test: return with a variable reference."""
        from waymark import action, workflow
        from waymark.workflow import Workflow

        @action(name="return_test_action")
        async def return_action() -> int:
            return 42

        @workflow
        class ReturnVarWorkflow(Workflow):
            async def run(self) -> int:
                result = await return_action()
                return result

        program = ReturnVarWorkflow.workflow_ir()

        # Find the return statement
        func = program.functions[0]
        return_stmt = None
        for stmt in func.body.statements:
            if stmt.HasField("return_stmt"):
                return_stmt = stmt.return_stmt
                break

        assert return_stmt is not None, "Should have return statement"
        assert return_stmt.value.HasField("variable"), "Return value should be variable"
        assert return_stmt.value.variable.name == "result", "Return should reference 'result'"

    def test_return_with_literal(self) -> None:
        """Test: return with a literal value."""
        from waymark import workflow
        from waymark.workflow import Workflow

        @workflow
        class ReturnLiteralWorkflow(Workflow):
            async def run(self) -> int:
                return 42

        program = ReturnLiteralWorkflow.workflow_ir()

        func = program.functions[0]
        return_stmt = None
        for stmt in func.body.statements:
            if stmt.HasField("return_stmt"):
                return_stmt = stmt.return_stmt
                break

        assert return_stmt is not None, "Should have return statement"
        assert return_stmt.value.HasField("literal"), "Return value should be literal"
        assert return_stmt.value.literal.int_value == 42, "Return should be 42"

    def test_return_without_value(self) -> None:
        """Test: return without a value."""
        from waymark import workflow
        from waymark.workflow import Workflow

        @workflow
        class ReturnNoneWorkflow(Workflow):
            async def run(self) -> None:
                return

        program = ReturnNoneWorkflow.workflow_ir()

        func = program.functions[0]
        return_stmt = None
        for stmt in func.body.statements:
            if stmt.HasField("return_stmt"):
                return_stmt = stmt.return_stmt
                break

        assert return_stmt is not None, "Should have return statement"


class TestAugmentedAssignment:
    """Test augmented assignment (+=, -=, etc.)."""

    def test_plus_equals_assignment(self) -> None:
        """Test: x += 1 is converted to x = x + 1."""
        from waymark import action, workflow
        from waymark.workflow import Workflow

        @action(name="aug_test_action")
        async def aug_action() -> int:
            return 5

        @workflow
        class PlusEqualsWorkflow(Workflow):
            async def run(self) -> int:
                x = await aug_action()
                x += 1
                return x

        program = PlusEqualsWorkflow.workflow_ir()

        func = program.functions[0]
        # Find the assignment with binary op
        aug_assign = None
        for stmt in func.body.statements:
            if stmt.HasField("assignment"):
                if stmt.assignment.value.HasField("binary_op"):
                    aug_assign = stmt.assignment
                    break

        assert aug_assign is not None, "Should have augmented assignment"
        assert aug_assign.targets == ["x"], "Target should be 'x'"
        assert aug_assign.value.binary_op.op == ir.BinaryOperator.BINARY_OP_ADD, "Op should be ADD"


class TestExpressionTypes:
    """Test various expression types in IR."""

    def test_list_expression(self) -> None:
        """Test: [1, 2, 3] list literals."""
        from waymark import workflow
        from waymark.workflow import Workflow

        @workflow
        class ListWorkflow(Workflow):
            async def run(self) -> list[int]:
                items = [1, 2, 3]
                return items

        program = ListWorkflow.workflow_ir()

        func = program.functions[0]
        # Find the assignment with list
        list_assign = None
        for stmt in func.body.statements:
            if stmt.HasField("assignment"):
                if stmt.assignment.value.HasField("list"):
                    list_assign = stmt.assignment
                    break

        assert list_assign is not None, "Should have list assignment"
        assert len(list_assign.value.list.elements) == 3, "Should have 3 elements"

    def test_dict_expression(self) -> None:
        """Test: {"key": "value"} dict literals."""
        from waymark import workflow
        from waymark.workflow import Workflow

        @workflow
        class DictWorkflow(Workflow):
            async def run(self) -> dict[str, int]:
                data = {"a": 1, "b": 2}
                return data

        program = DictWorkflow.workflow_ir()

        func = program.functions[0]
        # Find the assignment with dict
        dict_assign = None
        for stmt in func.body.statements:
            if stmt.HasField("assignment"):
                if stmt.assignment.value.HasField("dict"):
                    dict_assign = stmt.assignment
                    break

        assert dict_assign is not None, "Should have dict assignment"
        assert len(dict_assign.value.dict.entries) == 2, "Should have 2 entries"

    def test_index_expression(self) -> None:
        """Test: items[0] index access."""
        from waymark import workflow
        from waymark.workflow import Workflow

        @workflow
        class IndexWorkflow(Workflow):
            async def run(self, items: list[int]) -> int:
                first = items[0]
                return first

        program = IndexWorkflow.workflow_ir()

        func = program.functions[0]
        # Find the assignment with index
        index_assign = None
        for stmt in func.body.statements:
            if stmt.HasField("assignment"):
                if stmt.assignment.value.HasField("index"):
                    index_assign = stmt.assignment
                    break

        assert index_assign is not None, "Should have index assignment"
        assert index_assign.value.index.object.HasField("variable"), (
            "Index object should be variable"
        )
        assert index_assign.value.index.object.variable.name == "items", (
            "Index object should be 'items'"
        )

    def test_dot_expression(self) -> None:
        """Test: obj.attr dot access."""
        from waymark import workflow
        from waymark.workflow import Workflow

        @workflow
        class DotWorkflow(Workflow):
            async def run(self, obj: dict) -> object:
                # Use a simple attribute access pattern
                name = obj.get
                return name

        program = DotWorkflow.workflow_ir()
        # Just verify it builds without error
        assert program is not None

    def test_unary_not_expression(self) -> None:
        """Test: not x unary operator."""
        from waymark import workflow
        from waymark.workflow import Workflow

        @workflow
        class UnaryNotWorkflow(Workflow):
            async def run(self, flag: bool) -> bool:
                result = not flag
                return result

        program = UnaryNotWorkflow.workflow_ir()

        func = program.functions[0]
        # Find the assignment with unary op
        unary_assign = None
        for stmt in func.body.statements:
            if stmt.HasField("assignment"):
                if stmt.assignment.value.HasField("unary_op"):
                    unary_assign = stmt.assignment
                    break

        assert unary_assign is not None, "Should have unary assignment"
        assert unary_assign.value.unary_op.op == ir.UnaryOperator.UNARY_OP_NOT, "Op should be NOT"

    def test_comparison_operators(self) -> None:
        """Test: various comparison operators."""
        from waymark import workflow
        from waymark.workflow import Workflow

        @workflow
        class ComparisonWorkflow(Workflow):
            async def run(self, x: int, y: int) -> list[bool]:
                lt = x < y
                le = x <= y
                gt = x > y
                ge = x >= y
                eq = x == y
                ne = x != y
                return [lt, le, gt, ge, eq, ne]

        program = ComparisonWorkflow.workflow_ir()
        # Just verify it builds without error - all ops are covered
        assert program is not None
        func = program.functions[0]
        # Should have several assignments with binary ops
        binary_count = 0
        for stmt in func.body.statements:
            if stmt.HasField("assignment"):
                if stmt.assignment.value.HasField("binary_op"):
                    binary_count += 1
        assert binary_count >= 6, "Should have 6 comparison assignments"

    def test_boolean_operators(self) -> None:
        """Test: and/or boolean operators."""
        from waymark import workflow
        from waymark.workflow import Workflow

        @workflow
        class BooleanWorkflow(Workflow):
            async def run(self, a: bool, b: bool) -> list[bool]:
                and_result = a and b
                or_result = a or b
                return [and_result, or_result]

        program = BooleanWorkflow.workflow_ir()
        assert program is not None
        func = program.functions[0]
        # Should have assignments with binary ops
        binary_count = 0
        for stmt in func.body.statements:
            if stmt.HasField("assignment"):
                if stmt.assignment.value.HasField("binary_op"):
                    binary_count += 1
        assert binary_count >= 2, "Should have 2 boolean op assignments"

    def test_arithmetic_operators(self) -> None:
        """Test: +, -, *, / arithmetic operators."""
        from waymark import workflow
        from waymark.workflow import Workflow

        @workflow
        class ArithmeticWorkflow(Workflow):
            async def run(self, a: int, b: int) -> list[int]:
                add = a + b
                sub = a - b
                mul = a * b
                div = a // b
                return [add, sub, mul, div]

        program = ArithmeticWorkflow.workflow_ir()
        assert program is not None


class TestLiteralTypes:
    """Test literal type handling."""

    def test_string_literal(self) -> None:
        """Test: string literals."""
        from waymark import workflow
        from waymark.workflow import Workflow

        @workflow
        class StringWorkflow(Workflow):
            async def run(self) -> str:
                msg = "hello"
                return msg

        program = StringWorkflow.workflow_ir()

        func = program.functions[0]
        # Find the assignment with literal
        str_assign = None
        for stmt in func.body.statements:
            if stmt.HasField("assignment"):
                if stmt.assignment.value.HasField("literal"):
                    str_assign = stmt.assignment
                    break

        assert str_assign is not None, "Should have string assignment"
        assert str_assign.value.literal.string_value == "hello", "Should be 'hello'"

    def test_float_literal(self) -> None:
        """Test: float literals."""
        from waymark import workflow
        from waymark.workflow import Workflow

        @workflow
        class FloatWorkflow(Workflow):
            async def run(self) -> float:
                val = 3.14
                return val

        program = FloatWorkflow.workflow_ir()

        func = program.functions[0]
        # Find the assignment with literal
        float_assign = None
        for stmt in func.body.statements:
            if stmt.HasField("assignment"):
                if stmt.assignment.value.HasField("literal"):
                    float_assign = stmt.assignment
                    break

        assert float_assign is not None, "Should have float assignment"
        assert abs(float_assign.value.literal.float_value - 3.14) < 0.01, "Should be 3.14"

    def test_bool_literal(self) -> None:
        """Test: bool literals."""
        from waymark import workflow
        from waymark.workflow import Workflow

        @workflow
        class BoolWorkflow(Workflow):
            async def run(self) -> bool:
                val = True
                return val

        program = BoolWorkflow.workflow_ir()

        func = program.functions[0]
        # Find the assignment with literal
        bool_assign = None
        for stmt in func.body.statements:
            if stmt.HasField("assignment"):
                if stmt.assignment.value.HasField("literal"):
                    bool_assign = stmt.assignment
                    break

        assert bool_assign is not None, "Should have bool assignment"
        assert bool_assign.value.literal.bool_value is True, "Should be True"

    def test_none_literal(self) -> None:
        """Test: None literals."""
        from waymark import workflow
        from waymark.workflow import Workflow

        @workflow
        class NoneWorkflow(Workflow):
            async def run(self) -> None:
                val = None
                return val

        program = NoneWorkflow.workflow_ir()

        func = program.functions[0]
        # Find the assignment with literal
        none_assign = None
        for stmt in func.body.statements:
            if stmt.HasField("assignment"):
                if stmt.assignment.value.HasField("literal"):
                    none_assign = stmt.assignment
                    break

        assert none_assign is not None, "Should have None assignment"
        assert none_assign.value.literal.is_none is True, "Should be None"


class TestMoreUnsupportedPatterns:
    """Test additional unsupported patterns for coverage."""

    def test_dict_comprehension_raises_error(self) -> None:
        """Test: dict comprehensions raise error."""
        import pytest

        from waymark import UnsupportedPatternError, workflow
        from waymark.workflow import Workflow

        @workflow
        class DictCompWorkflow(Workflow):
            async def run(self, items: list[int]) -> dict[int, int]:
                return {x: x * 2 for x in items}

        with pytest.raises(UnsupportedPatternError) as exc_info:
            DictCompWorkflow.workflow_ir()

        error = exc_info.value
        assert isinstance(error, UnsupportedPatternError)
        assert "Dict comprehension" in error.message

    def test_set_comprehension_raises_error(self) -> None:
        """Test: set comprehensions raise error."""
        import pytest

        from waymark import UnsupportedPatternError, workflow
        from waymark.workflow import Workflow

        @workflow
        class SetCompWorkflow(Workflow):
            async def run(self, items: list[int]) -> set[int]:
                return {x * 2 for x in items}

        with pytest.raises(UnsupportedPatternError) as exc_info:
            SetCompWorkflow.workflow_ir()

        error = exc_info.value
        assert isinstance(error, UnsupportedPatternError)
        assert "Set comprehension" in error.message

    def test_generator_expression_raises_error(self) -> None:
        """Test: generator expressions raise error."""
        import pytest

        from waymark import UnsupportedPatternError, workflow
        from waymark.workflow import Workflow

        @workflow
        class GeneratorWorkflow(Workflow):
            async def run(self, items: list[int]) -> object:
                return (x * 2 for x in items)

        with pytest.raises(UnsupportedPatternError) as exc_info:
            GeneratorWorkflow.workflow_ir()

        error = exc_info.value
        assert isinstance(error, UnsupportedPatternError)
        assert "Generator" in error.message

    def test_walrus_operator_raises_error(self) -> None:
        """Test: walrus operator raises error."""
        import pytest

        from waymark import UnsupportedPatternError, workflow
        from waymark.workflow import Workflow

        @workflow
        class WalrusWorkflow(Workflow):
            async def run(self, items: list[int]) -> int:
                if (n := len(items)) > 0:
                    return n
                return 0

        with pytest.raises(UnsupportedPatternError) as exc_info:
            WalrusWorkflow.workflow_ir()

        error = exc_info.value
        assert isinstance(error, UnsupportedPatternError)
        assert "walrus" in error.message.lower()

    def test_match_statement_raises_error(self) -> None:
        """Test: match statements raise error (Python 3.10+)."""
        import sys

        if sys.version_info < (3, 10):
            return  # Skip on older Python versions

        import pytest

        # Use fixture file to test match statement (requires source code access)
        from tests.fixtures_unsupported.match_workflow import MatchWorkflow
        from waymark import UnsupportedPatternError

        with pytest.raises(UnsupportedPatternError) as exc_info:
            MatchWorkflow.workflow_ir()

        error = exc_info.value
        assert isinstance(error, UnsupportedPatternError)
        assert "Match" in error.message


class TestForLoopEnumerate:
    """Test for loop with enumerate pattern."""

    def test_for_enumerate_unpacking(self) -> None:
        """Test: for i, item in enumerate(items) creates correct loop vars."""
        from waymark import action, workflow
        from waymark.workflow import Workflow

        @action(name="enumerate_action")
        async def process_item(idx: int, item: str) -> str:
            return f"{idx}: {item}"

        @workflow
        class EnumerateWorkflow(Workflow):
            async def run(self, items: list[str]) -> list[str]:
                results = []
                for i, item in enumerate(items):
                    result = await process_item(idx=i, item=item)
                    results.append(result)
                return results

        program = EnumerateWorkflow.workflow_ir()

        # Find the for loop (may not be in functions[0] if implicit functions are created)
        for_loop = None
        for func in program.functions:
            for stmt in func.body.statements:
                if stmt.HasField("for_loop"):
                    for_loop = stmt.for_loop
                    break
            if for_loop is not None:
                break

        assert for_loop is not None, "Should have for loop"
        # enumerate unpacks to two loop vars
        assert len(for_loop.loop_vars) == 2, "Should have 2 loop vars (i, item)"
        assert "i" in for_loop.loop_vars, "Should have 'i' loop var"
        assert "item" in for_loop.loop_vars, "Should have 'item' loop var"
        assert (
            for_loop.iterable.function_call.global_function
            == ir.GlobalFunction.GLOBAL_FUNCTION_ENUMERATE
        )


class TestExprStmt:
    """Test expression statements (side-effect only)."""

    def test_action_call_without_assignment(self) -> None:
        """Test: await action() without assignment uses action from fixture file."""
        # Uses a fixture with module-level actions because action discovery
        # requires actions to be at module level
        from tests.fixtures_side_effects.side_effect_action import SideEffectWorkflow

        program = SideEffectWorkflow.workflow_ir()

        func = program.functions[0]
        # Find action call statement (not in assignment)
        action_stmt = None
        for stmt in func.body.statements:
            if stmt.HasField("action_call"):
                action_stmt = stmt
                break

        assert action_stmt is not None, (
            f"Should have side-effect action call, got: {[s.WhichOneof('kind') for s in func.body.statements]}"
        )
        assert action_stmt.action_call.action_name == "side_effect"


class TestUnaryOperators:
    """Test unary operators."""

    def test_unary_minus(self) -> None:
        """Test: -x unary negation."""
        from waymark import workflow
        from waymark.workflow import Workflow

        @workflow
        class UnaryMinusWorkflow(Workflow):
            async def run(self, x: int) -> int:
                result = -x
                return result

        program = UnaryMinusWorkflow.workflow_ir()

        func = program.functions[0]
        # Find unary op
        unary_assign = None
        for stmt in func.body.statements:
            if stmt.HasField("assignment"):
                if stmt.assignment.value.HasField("unary_op"):
                    unary_assign = stmt.assignment
                    break

        assert unary_assign is not None, "Should have unary assignment"
        assert unary_assign.value.unary_op.op == ir.UnaryOperator.UNARY_OP_NEG


class TestElseBranch:
    """Test else branches in conditionals."""

    def test_if_else_with_actions(self) -> None:
        """Test: if/else with action in else branch."""
        from waymark import action, workflow
        from waymark.workflow import Workflow

        @action(name="if_action")
        async def if_action() -> int:
            return 1

        @action(name="else_action")
        async def else_action() -> int:
            return 2

        @workflow
        class IfElseActionWorkflow(Workflow):
            async def run(self, flag: bool) -> int:
                if flag:
                    result = await if_action()
                else:
                    result = await else_action()
                return result

        program = IfElseActionWorkflow.workflow_ir()

        # Find conditional (may not be in functions[0] if implicit functions are created)
        conditional = None
        for func in program.functions:
            for stmt in func.body.statements:
                if stmt.HasField("conditional"):
                    conditional = stmt.conditional
                    break
            if conditional is not None:
                break

        assert conditional is not None, "Should have conditional"
        assert conditional.HasField("if_branch"), "Should have if branch"
        # Should have else branch
        assert len(conditional.elif_branches) == 0 or conditional.HasField("else_branch"), (
            "Should have else branch"
        )


class TestMoreBinaryOperators:
    """Test more binary operator coverage."""

    def test_modulo_operator(self) -> None:
        """Test: x % y modulo operator."""
        from waymark import workflow
        from waymark.workflow import Workflow

        @workflow
        class ModuloWorkflow(Workflow):
            async def run(self, x: int, y: int) -> int:
                return x % y

        program = ModuloWorkflow.workflow_ir()
        assert program is not None

    def test_power_operator(self) -> None:
        """Test: x ** y power operator."""
        import pytest

        from waymark import UnsupportedPatternError, workflow
        from waymark.workflow import Workflow

        @workflow
        class PowerWorkflow(Workflow):
            async def run(self, x: int, y: int) -> int:
                return x**y

        with pytest.raises(UnsupportedPatternError):
            PowerWorkflow.workflow_ir()

    def test_floor_division(self) -> None:
        """Test: x // y floor division."""
        from waymark import workflow
        from waymark.workflow import Workflow

        @workflow
        class FloorDivWorkflow(Workflow):
            async def run(self, x: int, y: int) -> int:
                return x // y

        program = FloorDivWorkflow.workflow_ir()
        assert program is not None

    def test_true_division(self) -> None:
        """Test: x / y true division."""
        from waymark import workflow
        from waymark.workflow import Workflow

        @workflow
        class TrueDivWorkflow(Workflow):
            async def run(self, x: float, y: float) -> float:
                return x / y

        program = TrueDivWorkflow.workflow_ir()
        assert program is not None


class TestNestedExpressions:
    """Test nested expression handling."""

    def test_nested_binary_ops(self) -> None:
        """Test: (a + b) * c nested operations."""
        from waymark import workflow
        from waymark.workflow import Workflow

        @workflow
        class NestedOpsWorkflow(Workflow):
            async def run(self, a: int, b: int, c: int) -> int:
                result = (a + b) * c
                return result

        program = NestedOpsWorkflow.workflow_ir()

        func = program.functions[0]
        # Should have assignment with binary op
        found = False
        for stmt in func.body.statements:
            if stmt.HasField("assignment"):
                if stmt.assignment.value.HasField("binary_op"):
                    found = True
                    break

        assert found, "Should have nested binary ops"

    def test_list_with_expressions(self) -> None:
        """Test: [a + 1, b * 2] list with expressions."""
        from waymark import workflow
        from waymark.workflow import Workflow

        @workflow
        class ListExprWorkflow(Workflow):
            async def run(self, a: int, b: int) -> list[int]:
                result = [a + 1, b * 2]
                return result

        program = ListExprWorkflow.workflow_ir()

        func = program.functions[0]
        # Find list assignment
        list_assign = None
        for stmt in func.body.statements:
            if stmt.HasField("assignment"):
                if stmt.assignment.value.HasField("list"):
                    list_assign = stmt.assignment
                    break

        assert list_assign is not None, "Should have list"
        assert len(list_assign.value.list.elements) == 2, "Should have 2 elements"


class TestAugmentedAssignmentTypes:
    """Test different augmented assignment operators."""

    def test_minus_equals(self) -> None:
        """Test: x -= 1"""
        from waymark import workflow
        from waymark.workflow import Workflow

        @workflow
        class MinusEqualsWorkflow(Workflow):
            async def run(self) -> int:
                x = 10
                x -= 1
                return x

        program = MinusEqualsWorkflow.workflow_ir()

        func = program.functions[0]
        # Find augmented assignment with SUB
        found = False
        for stmt in func.body.statements:
            if stmt.HasField("assignment"):
                if stmt.assignment.value.HasField("binary_op"):
                    if stmt.assignment.value.binary_op.op == ir.BinaryOperator.BINARY_OP_SUB:
                        found = True
                        break

        assert found, "Should have -= converted to binary sub"

    def test_times_equals(self) -> None:
        """Test: x *= 2"""
        from waymark import workflow
        from waymark.workflow import Workflow

        @workflow
        class TimesEqualsWorkflow(Workflow):
            async def run(self) -> int:
                x = 5
                x *= 2
                return x

        program = TimesEqualsWorkflow.workflow_ir()

        func = program.functions[0]
        # Find augmented assignment with MUL
        found = False
        for stmt in func.body.statements:
            if stmt.HasField("assignment"):
                if stmt.assignment.value.HasField("binary_op"):
                    if stmt.assignment.value.binary_op.op == ir.BinaryOperator.BINARY_OP_MUL:
                        found = True
                        break

        assert found, "Should have *= converted to binary mul"


class TestCallInTryBody:
    """Test call handling in try body."""

    def test_try_with_function_call(self) -> None:
        """Test: try body with action call uses fixture."""
        # Uses fixture with module-level action
        from tests.fixtures_try_except.try_with_action import TryWithActionWorkflow

        program = TryWithActionWorkflow.workflow_ir()

        # Find try/except (may not be in functions[0] if implicit functions are created)
        try_except = None
        for func in program.functions:
            for stmt in func.body.statements:
                if stmt.HasField("try_except"):
                    try_except = stmt.try_except
                    break
            if try_except is not None:
                break

        assert try_except is not None, "Should have try/except"
        assert try_except.HasField("try_block"), "Try body should have try_block"

        found_action = False
        for stmt in try_except.try_block.statements:
            if (
                stmt.HasField("assignment")
                and stmt.assignment.value.HasField("action_call")
                and stmt.assignment.value.action_call.action_name == "try_action"
            ):
                assert list(stmt.assignment.targets) == ["result"], (
                    f"Expected targets=['result'], got: {list(stmt.assignment.targets)}"
                )
                found_action = True
                break
        assert found_action, "Should have try_action call in try block"


class TestPolicyVariations:
    """Test various policy configurations for coverage."""

    def _find_action_by_name(self, program: ir.Program, action_name: str) -> ir.ActionCall | None:
        """Find an action call by name."""
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("action_call"):
                    if stmt.action_call.action_name == action_name:
                        return stmt.action_call
                elif stmt.HasField("assignment"):
                    if stmt.assignment.value.HasField("action_call"):
                        if stmt.assignment.value.action_call.action_name == action_name:
                            return stmt.assignment.value.action_call
        return None

    def test_timeout_with_direct_integer(self) -> None:
        """Test: timeout=60 (direct integer, not timedelta)."""
        from tests.fixtures_policy.policy_variations import PolicyVariationsWorkflow

        program = PolicyVariationsWorkflow.workflow_ir()

        action = self._find_action_by_name(program, "action_with_timeout_int")
        assert action is not None, "Should find action_with_timeout_int"
        assert len(action.policies) == 1, "Should have 1 policy"

        policy = action.policies[0]
        assert policy.HasField("timeout"), "Should be timeout policy"
        assert policy.timeout.timeout.seconds == 60

    def test_timeout_with_timedelta_minutes(self) -> None:
        """Test: timeout=timedelta(minutes=2)."""
        from tests.fixtures_policy.policy_variations import PolicyVariationsWorkflow

        program = PolicyVariationsWorkflow.workflow_ir()

        action = self._find_action_by_name(program, "action_with_timeout_minutes")
        assert action is not None, "Should find action_with_timeout_minutes"
        assert len(action.policies) == 1, "Should have 1 policy"

        policy = action.policies[0]
        assert policy.HasField("timeout"), "Should be timeout policy"
        assert policy.timeout.timeout.seconds == 120  # 2 minutes

    def test_retry_with_backoff_seconds(self) -> None:
        """Test: retry=RetryPolicy(attempts=3, backoff_seconds=5)."""
        from tests.fixtures_policy.policy_variations import PolicyVariationsWorkflow

        program = PolicyVariationsWorkflow.workflow_ir()

        action = self._find_action_by_name(program, "action_with_retry_backoff")
        assert action is not None, "Should find action_with_retry_backoff"
        assert len(action.policies) == 1, "Should have 1 policy"

        policy = action.policies[0]
        assert policy.HasField("retry"), "Should be retry policy"
        # attempts=3 means 3 total executions, so max_retries=2
        assert policy.retry.max_retries == 2
        assert policy.retry.backoff.seconds == 5

    def test_retry_with_exception_types(self) -> None:
        """Test: retry=RetryPolicy(attempts=2, exception_types=["ValueError", "KeyError"])."""
        from tests.fixtures_policy.policy_variations import PolicyVariationsWorkflow

        program = PolicyVariationsWorkflow.workflow_ir()

        action = self._find_action_by_name(program, "action_with_retry_exceptions")
        assert action is not None, "Should find action_with_retry_exceptions"
        assert len(action.policies) == 1, "Should have 1 policy"

        policy = action.policies[0]
        assert policy.HasField("retry"), "Should be retry policy"
        # attempts=2 means 2 total executions, so max_retries=1
        assert policy.retry.max_retries == 1
        assert "ValueError" in policy.retry.exception_types
        assert "KeyError" in policy.retry.exception_types

    def test_retry_without_attempts_uses_default_max_retries(self) -> None:
        """Test: retry=RetryPolicy() uses the default max retries."""
        from tests.fixtures_policy.policy_variations import PolicyVariationsWorkflow

        program = PolicyVariationsWorkflow.workflow_ir()

        action = self._find_action_by_name(program, "action_with_retry_default")
        assert action is not None, "Should find action_with_retry_default"
        assert len(action.policies) == 1, "Should have 1 policy"

        policy = action.policies[0]
        assert policy.HasField("retry"), "Should be retry policy"
        assert policy.retry.max_retries == 100

    def test_timeout_with_timedelta_hours(self) -> None:
        """Test: timeout=timedelta(hours=1)."""
        from tests.fixtures_policy.policy_variations import PolicyVariationsWorkflow

        program = PolicyVariationsWorkflow.workflow_ir()

        action = self._find_action_by_name(program, "action_with_timeout_hours")
        assert action is not None, "Should find action_with_timeout_hours"
        assert len(action.policies) == 1, "Should have 1 policy"

        policy = action.policies[0]
        assert policy.HasField("timeout"), "Should be timeout policy"
        assert policy.timeout.timeout.seconds == 3600  # 1 hour

    def test_timeout_with_timedelta_days(self) -> None:
        """Test: timeout=timedelta(days=1)."""
        from tests.fixtures_policy.policy_variations import PolicyVariationsWorkflow

        program = PolicyVariationsWorkflow.workflow_ir()

        action = self._find_action_by_name(program, "action_with_timeout_days")
        assert action is not None, "Should find action_with_timeout_days"
        assert len(action.policies) == 1, "Should have 1 policy"

        policy = action.policies[0]
        assert policy.HasField("timeout"), "Should be timeout policy"
        assert policy.timeout.timeout.seconds == 86400  # 1 day


class TestInstanceAttrPolicies:
    """Test policies stored as instance attributes and referenced via self.attr."""

    def _find_action_by_name(self, program: ir.Program, action_name: str) -> ir.ActionCall | None:
        """Find an action call by name."""
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("action_call"):
                    if stmt.action_call.action_name == action_name:
                        return stmt.action_call
                elif stmt.HasField("assignment"):
                    if stmt.assignment.value.HasField("action_call"):
                        if stmt.assignment.value.action_call.action_name == action_name:
                            return stmt.assignment.value.action_call
        return None

    def test_retry_policy_from_instance_attr(self) -> None:
        """Test: retry=self.retry_policy resolves to RetryPolicy from __init__."""
        from tests.fixtures_policy.instance_attr_policies import InstanceAttrPoliciesWorkflow

        program = InstanceAttrPoliciesWorkflow.workflow_ir()

        action = self._find_action_by_name(program, "action_with_instance_retry")
        assert action is not None, "Should find action_with_instance_retry"
        assert len(action.policies) == 1, "Should have 1 policy"

        policy = action.policies[0]
        assert policy.HasField("retry"), "Should be retry policy"
        # attempts=3 means 3 total executions, so max_retries=2
        assert policy.retry.max_retries == 2
        assert policy.retry.backoff.seconds == 10

    def test_timeout_from_instance_attr(self) -> None:
        """Test: timeout=self.timeout_value resolves to integer from __init__."""
        from tests.fixtures_policy.instance_attr_policies import InstanceAttrPoliciesWorkflow

        program = InstanceAttrPoliciesWorkflow.workflow_ir()

        action = self._find_action_by_name(program, "action_with_instance_timeout")
        assert action is not None, "Should find action_with_instance_timeout"
        assert len(action.policies) == 1, "Should have 1 policy"

        policy = action.policies[0]
        assert policy.HasField("timeout"), "Should be timeout policy"
        assert policy.timeout.timeout.seconds == 120

    def test_both_policies_from_instance_attrs(self) -> None:
        """Test: both retry and timeout from instance attributes."""
        from tests.fixtures_policy.instance_attr_policies import InstanceAttrPoliciesWorkflow

        program = InstanceAttrPoliciesWorkflow.workflow_ir()

        action = self._find_action_by_name(program, "action_with_both_policies")
        assert action is not None, "Should find action_with_both_policies"
        assert len(action.policies) == 2, "Should have 2 policies"

        # Find retry and timeout policies
        retry_policy = None
        timeout_policy = None
        for policy in action.policies:
            if policy.HasField("retry"):
                retry_policy = policy.retry
            elif policy.HasField("timeout"):
                timeout_policy = policy.timeout

        assert retry_policy is not None, "Should have retry policy"
        assert timeout_policy is not None, "Should have timeout policy"

        # fast_retry has attempts=2, so max_retries=1
        assert retry_policy.max_retries == 1
        # timeout_value = 120
        assert timeout_policy.timeout.seconds == 120


class TestSpreadAction:
    """Test spread action detection - converts to SpreadExpr in IR."""

    def test_spread_pattern_converts_to_spread_expr(self) -> None:
        """Test: asyncio.gather(*[action(item) for item in items], return_exceptions=True) -> SpreadExpr."""
        from tests.fixtures_gather.gather_listcomp import GatherListCompWorkflow

        program = GatherListCompWorkflow.workflow_ir()

        # Find the SpreadExpr in the IR
        spread_found = False
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("assignment"):
                    if stmt.assignment.value.HasField("spread_expr"):
                        spread_found = True
                        spread_expr = stmt.assignment.value.spread_expr
                        # Verify the spread structure
                        assert spread_expr.loop_var == "item"
                        assert spread_expr.action.action_name == "process_item"

        assert spread_found, "Expected spread expression from asyncio.gather(*[...])"

    def test_spread_pattern_with_run_action(self) -> None:
        """Test: asyncio.gather(*[self.run_action(action(x), retry=..., timeout=...) for x in items], return_exceptions=True).

        This tests the pattern where run_action wraps the action call to add
        retry and timeout policies in a spread pattern.
        """
        from tests.fixtures_gather.gather_run_action_spread import (
            GatherRunActionSpreadWorkflow,
        )

        program = GatherRunActionSpreadWorkflow.workflow_ir()

        # Find the SpreadExpr in the IR
        spread_found = False
        spread_expr = None
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("assignment"):
                    if stmt.assignment.value.HasField("spread_expr"):
                        spread_found = True
                        spread_expr = stmt.assignment.value.spread_expr

        assert spread_found, (
            "Expected spread expression from asyncio.gather(*[self.run_action(...)])"
        )
        assert spread_expr is not None

        # Verify the spread structure
        assert spread_expr.loop_var == "item"
        assert spread_expr.action.action_name == "process_item"

        # Verify that policies were extracted from run_action
        assert len(spread_expr.action.policies) == 2, (
            f"Expected 2 policies (retry + timeout), got {len(spread_expr.action.policies)}"
        )

        # Check for retry policy
        retry_found = False
        timeout_found = False
        for policy_bracket in spread_expr.action.policies:
            if policy_bracket.HasField("retry"):
                retry_found = True
                # RetryPolicy(attempts=3) -> max_retries=2
                assert policy_bracket.retry.max_retries == 2
            if policy_bracket.HasField("timeout"):
                timeout_found = True
                # timedelta(seconds=30) -> 30 seconds
                assert policy_bracket.timeout.timeout.seconds == 30

        assert retry_found, "Expected retry policy from run_action"
        assert timeout_found, "Expected timeout policy from run_action"

    def test_static_gather_with_run_action(self) -> None:
        """Test: asyncio.gather(self.run_action(...), self.run_action(...), return_exceptions=True).

        This tests the pattern where run_action wraps static action calls to add
        retry and timeout policies.
        """
        from tests.fixtures_gather.gather_run_action_static import (
            GatherRunActionStaticWorkflow,
        )

        program = GatherRunActionStaticWorkflow.workflow_ir()

        # Find the ParallelExpr in the IR
        parallel_found = False
        parallel_expr = None
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("assignment"):
                    if stmt.assignment.value.HasField("parallel_expr"):
                        parallel_found = True
                        parallel_expr = stmt.assignment.value.parallel_expr

        assert parallel_found, (
            "Expected parallel expression from asyncio.gather(self.run_action(...))"
        )
        assert parallel_expr is not None

        # Should have 2 calls
        assert len(parallel_expr.calls) == 2, (
            f"Expected 2 calls in parallel, got {len(parallel_expr.calls)}"
        )

        # Verify first action (upload_ci_logs)
        call1 = parallel_expr.calls[0]
        assert call1.HasField("action"), "Expected first call to be an action"
        assert call1.action.action_name == "upload_ci_logs"
        assert len(call1.action.policies) == 2, (
            f"Expected 2 policies for upload_ci_logs, got {len(call1.action.policies)}"
        )

        # Check policies for first action
        retry_found = False
        timeout_found = False
        for policy_bracket in call1.action.policies:
            if policy_bracket.HasField("retry"):
                retry_found = True
                # RetryPolicy(attempts=3) -> max_retries=2
                assert policy_bracket.retry.max_retries == 2
            if policy_bracket.HasField("timeout"):
                timeout_found = True
                # timedelta(minutes=2) -> 120 seconds
                assert policy_bracket.timeout.timeout.seconds == 120

        assert retry_found, "Expected retry policy for upload_ci_logs"
        assert timeout_found, "Expected timeout policy for upload_ci_logs"

        # Verify second action (cleanup_ci_sandbox)
        call2 = parallel_expr.calls[1]
        assert call2.HasField("action"), "Expected second call to be an action"
        assert call2.action.action_name == "cleanup_ci_sandbox"
        assert len(call2.action.policies) == 2, (
            f"Expected 2 policies for cleanup_ci_sandbox, got {len(call2.action.policies)}"
        )

        # Check policies for second action
        retry_found = False
        timeout_found = False
        for policy_bracket in call2.action.policies:
            if policy_bracket.HasField("retry"):
                retry_found = True
                # RetryPolicy(attempts=2) -> max_retries=1
                assert policy_bracket.retry.max_retries == 1
            if policy_bracket.HasField("timeout"):
                timeout_found = True
                # timedelta(minutes=1) -> 60 seconds
                assert policy_bracket.timeout.timeout.seconds == 60

        assert retry_found, "Expected retry policy for cleanup_ci_sandbox"
        assert timeout_found, "Expected timeout policy for cleanup_ci_sandbox"

    def test_static_gather_mixed_run_action(self) -> None:
        """Test: asyncio.gather(self.run_action(...), action(...), return_exceptions=True).

        This tests mixed patterns where some calls use run_action wrapper and others don't.
        """
        from tests.fixtures_gather.gather_run_action_static import (
            GatherMixedRunActionWorkflow,
        )

        program = GatherMixedRunActionWorkflow.workflow_ir()

        # Find the ParallelExpr in the IR
        parallel_found = False
        parallel_expr = None
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("assignment"):
                    if stmt.assignment.value.HasField("parallel_expr"):
                        parallel_found = True
                        parallel_expr = stmt.assignment.value.parallel_expr

        assert parallel_found, "Expected parallel expression from asyncio.gather"
        assert parallel_expr is not None

        # Should have 2 calls
        assert len(parallel_expr.calls) == 2, (
            f"Expected 2 calls in parallel, got {len(parallel_expr.calls)}"
        )

        # Verify first action (upload_ci_logs with run_action wrapper)
        call1 = parallel_expr.calls[0]
        assert call1.HasField("action"), "Expected first call to be an action"
        assert call1.action.action_name == "upload_ci_logs"
        # Should have 1 policy (retry only, no timeout)
        assert len(call1.action.policies) == 1, (
            f"Expected 1 policy for upload_ci_logs, got {len(call1.action.policies)}"
        )
        assert call1.action.policies[0].HasField("retry")
        # RetryPolicy(attempts=3) -> max_retries=2
        assert call1.action.policies[0].retry.max_retries == 2

        # Verify second action (send_notification without run_action wrapper)
        call2 = parallel_expr.calls[1]
        assert call2.HasField("action"), "Expected second call to be an action"
        assert call2.action.action_name == "send_notification"
        # Should have no policies (direct call without wrapper)
        assert len(call2.action.policies) == 0, (
            f"Expected 0 policies for send_notification, got {len(call2.action.policies)}"
        )


class TestForLoopWithMultipleCalls:
    """Test for loop bodies with multiple calls."""

    def test_for_body_is_block(self) -> None:
        """Test: for loop with multiple calls stays as a block (no synthetic function)."""
        from tests.fixtures_for_loop.for_multiple_calls import ForMultipleCallsWorkflow

        program = ForMultipleCallsWorkflow.workflow_ir()

        assert not any(fn.name.startswith("__for_body") for fn in program.functions), (
            "Did not expect implicit __for_body* functions"
        )

        # Find the for loop
        for_loop = None
        for fn in program.functions:
            if fn.name == "main":
                for stmt in fn.body.statements:
                    if stmt.HasField("for_loop"):
                        for_loop = stmt.for_loop
                        break

        assert for_loop is not None, "Should have for loop"

        assert for_loop.HasField("block_body"), "Expected block_body in for loop"
        assert len(for_loop.block_body.statements) >= 2, "Expected multi-statement loop body"


class TestConditionalWithMultipleCalls:
    """Test conditionals with multiple calls."""

    def test_if_body_is_block(self) -> None:
        """Test: if branch with multiple calls stays as a block (no synthetic function)."""
        from tests.fixtures_conditional.if_multiple_calls import IfMultipleCallsWorkflow

        program = IfMultipleCallsWorkflow.workflow_ir()

        assert not any(fn.name.startswith("__if_") for fn in program.functions), (
            "Did not expect implicit __if_* functions"
        )
        conditional = next(
            (
                stmt.conditional
                for stmt in iter_all_statements(program)
                if stmt.HasField("conditional")
            ),
            None,
        )
        assert conditional is not None, "Expected conditional in IR"
        assert conditional.if_branch.HasField("block_body"), "Expected block_body in if branch"
        assert len(conditional.if_branch.block_body.statements) >= 2, (
            "Expected multi-statement if branch"
        )


class TestAwaitActionInIfCondition:
    def test_if_await_action_condition_is_normalized(self) -> None:
        from tests.fixtures_conditional.if_await_action_condition import (
            IfAwaitActionConditionWorkflow,
        )

        program = IfAwaitActionConditionWorkflow.workflow_ir()

        run_fn = next((fn for fn in program.functions if fn.name == "main"), None)
        assert run_fn is not None, "Expected main() function"
        assert len(run_fn.body.statements) >= 2, "Expected hoisted condition assignment + if"

        assign_stmt = run_fn.body.statements[0]
        assert assign_stmt.HasField("assignment"), "Expected assignment for condition action"
        assert assign_stmt.assignment.value.HasField("action_call"), "Expected action_call RHS"
        assert assign_stmt.assignment.value.action_call.action_name == "is_even"
        assert len(assign_stmt.assignment.targets) == 1
        cond_var = assign_stmt.assignment.targets[0]
        assert cond_var.startswith("__if_cond_"), f"Unexpected condition var name: {cond_var}"

        if_stmt = run_fn.body.statements[1]
        assert if_stmt.HasField("conditional"), "Expected conditional after hoisted assignment"
        assert if_stmt.conditional.if_branch.condition.HasField("variable")
        assert if_stmt.conditional.if_branch.condition.variable.name == cond_var


class TestTryExceptWithMultipleCalls:
    """Test try/except with multiple calls."""

    def test_try_body_is_block(self) -> None:
        """Test: try body with multiple calls stays as a block (no synthetic function)."""
        from tests.fixtures_try_except.try_multiple_calls import TryMultipleCallsWorkflow

        program = TryMultipleCallsWorkflow.workflow_ir()

        assert not any(fn.name.startswith("__try_body_") for fn in program.functions), (
            "Did not expect implicit __try_body_* functions"
        )
        try_except = next(
            (
                stmt.try_except
                for stmt in iter_all_statements(program)
                if stmt.HasField("try_except")
            ),
            None,
        )
        assert try_except is not None, "Expected try/except in IR"
        assert try_except.HasField("try_block"), "Expected try_block"
        assert len(try_except.try_block.statements) >= 2, "Expected multi-statement try body"


class TestTryExceptStatefulOutputs:
    """Ensure try/except bodies emit assignments directly."""

    def test_try_body_outputs_and_call_targets(self) -> None:
        """Try and handler blocks should contain mutated variable assignments."""
        from tests.fixtures_try_except.try_stateful_outputs import TryStatefulOutputsWorkflow

        program = TryStatefulOutputsWorkflow.workflow_ir()

        run_fn = next(fn for fn in program.functions if fn.name == "main")
        try_stmt = next(
            stmt for stmt in run_fn.body.statements if stmt.HasField("try_except")
        ).try_except

        assert try_stmt.HasField("try_block"), "Expected try_block"
        try_assign_targets = {
            target
            for stmt in try_stmt.try_block.statements
            if stmt.HasField("assignment")
            for target in stmt.assignment.targets
        }
        assert {"value", "message"} <= try_assign_targets, (
            f"Expected value/message assigned in try block, got {try_assign_targets}"
        )

        handler = try_stmt.handlers[0]
        assert handler.HasField("block_body"), "Expected handler block_body"
        handler_assign_targets = {
            target
            for stmt in handler.block_body.statements
            if stmt.HasField("assignment")
            for target in stmt.assignment.targets
        }
        assert {"recovered", "message"} <= handler_assign_targets, (
            f"Expected recovered/message assigned in handler, got {handler_assign_targets}"
        )

        assert not any(fn.name.startswith("__try_body_") for fn in program.functions)
        assert not any(fn.name.startswith("__except_handler_") for fn in program.functions)

        # Final action should receive recovered/message kwargs (no missing inputs)
        build_action = next(
            stmt
            for stmt in run_fn.body.statements
            if stmt.HasField("assignment")
            and stmt.assignment.value.HasField("action_call")
            and stmt.assignment.value.action_call.action_name == "finalize_result"
        ).assignment.value.action_call
        kwarg_names = {kw.name for kw in build_action.kwargs}
        assert {"attempted", "recovered", "message"} <= kwarg_names


class TestUnsupportedPatternValidation:
    """Test that unsupported patterns raise UnsupportedPatternError with helpful messages."""

    def test_constructor_return_raises_error(self) -> None:
        """Test: return CustomClass(...) raises UnsupportedPatternError.

        Note: Pydantic models and dataclasses ARE supported. This test uses
        a regular class to ensure unsupported constructors still fail.
        """
        from typing import cast

        import pytest

        from waymark.ir_builder import UnsupportedPatternError

        with pytest.raises(UnsupportedPatternError) as exc_info:
            from tests.fixtures_unsupported.constructor_return import ConstructorReturnWorkflow

            ConstructorReturnWorkflow.workflow_ir()

        error = cast(UnsupportedPatternError, exc_info.value)
        assert "CustomResult" in error.message
        assert (
            "constructor" in error.message.lower() or "constructor" in error.recommendation.lower()
        )
        assert "@action" in error.recommendation

    def test_constructor_assignment_raises_error(self) -> None:
        """Test: x = CustomClass(...) raises UnsupportedPatternError.

        Note: Pydantic models and dataclasses ARE supported. This test uses
        a regular class to ensure unsupported constructors still fail.
        """
        from typing import cast

        import pytest

        from waymark.ir_builder import UnsupportedPatternError

        with pytest.raises(UnsupportedPatternError) as exc_info:
            from tests.fixtures_unsupported.constructor_assignment import (
                ConstructorAssignmentWorkflow,
            )

            ConstructorAssignmentWorkflow.workflow_ir()

        error = cast(UnsupportedPatternError, exc_info.value)
        assert "CustomConfig" in error.message
        assert "@action" in error.recommendation

    def test_non_action_await_raises_error(self) -> None:
        """Test: await non_action_func() raises UnsupportedPatternError."""
        from typing import cast

        import pytest

        from waymark.ir_builder import UnsupportedPatternError

        with pytest.raises(UnsupportedPatternError) as exc_info:
            from tests.fixtures_unsupported.non_action_await import NonActionAwaitWorkflow

            NonActionAwaitWorkflow.workflow_ir()

        error = cast(UnsupportedPatternError, exc_info.value)
        assert "helper_function" in error.message
        assert "non-action" in error.message.lower() or "@action" in error.recommendation

    def test_fstring_raises_error(self) -> None:
        """Test: f-strings raise UnsupportedPatternError."""
        from typing import cast

        import pytest

        from waymark.ir_builder import UnsupportedPatternError

        with pytest.raises(UnsupportedPatternError) as exc_info:
            from tests.fixtures_unsupported.fstring_usage import FstringWorkflow

            FstringWorkflow.workflow_ir()

        error = cast(UnsupportedPatternError, exc_info.value)
        assert "f-string" in error.message.lower() or "F-string" in error.message
        assert "@action" in error.recommendation

    def test_list_comprehension_return_raises_error(self) -> None:
        """Test: list comprehensions returned directly raise UnsupportedPatternError."""
        from typing import cast

        import pytest

        from waymark.ir_builder import UnsupportedPatternError

        with pytest.raises(UnsupportedPatternError) as exc_info:
            from tests.fixtures_unsupported.list_comprehension import ListComprehensionWorkflow

            ListComprehensionWorkflow.workflow_ir()

        error = cast(UnsupportedPatternError, exc_info.value)
        assert "comprehension" in error.message.lower()

    def test_lambda_raises_error(self) -> None:
        """Test: lambda expressions raise UnsupportedPatternError."""
        from typing import cast

        import pytest

        from waymark.ir_builder import UnsupportedPatternError

        with pytest.raises(UnsupportedPatternError) as exc_info:
            from tests.fixtures_unsupported.lambda_expression import LambdaExpressionWorkflow

            LambdaExpressionWorkflow.workflow_ir()

        error = cast(UnsupportedPatternError, exc_info.value)
        assert "lambda" in error.message.lower()
        assert "@action" in error.recommendation

    def test_with_statement_raises_error(self) -> None:
        """Test: with statements raise UnsupportedPatternError."""
        from typing import cast

        import pytest

        from waymark.ir_builder import UnsupportedPatternError

        with pytest.raises(UnsupportedPatternError) as exc_info:
            from tests.fixtures_unsupported.with_statement import WithStatementWorkflow

            WithStatementWorkflow.workflow_ir()

        error = cast(UnsupportedPatternError, exc_info.value)
        assert "with" in error.message.lower() or "context" in error.message.lower()
        assert "@action" in error.recommendation

    def test_match_statement_raises_error(self) -> None:
        """Test: match statements raise UnsupportedPatternError."""
        from typing import cast

        import pytest

        from waymark.ir_builder import UnsupportedPatternError

        with pytest.raises(UnsupportedPatternError) as exc_info:
            from tests.fixtures_unsupported.match_workflow import MatchWorkflow

            MatchWorkflow.workflow_ir()

        error = cast(UnsupportedPatternError, exc_info.value)
        assert "match" in error.message.lower()
        assert "if/elif/else" in error.recommendation.lower()

    def test_sync_function_call_assignment_raises_error(self) -> None:
        """Test: assigning a sync function call raises UnsupportedPatternError."""
        from datetime import datetime
        from typing import cast

        import pytest

        from waymark import workflow
        from waymark.ir_builder import UnsupportedPatternError
        from waymark.workflow import Workflow

        @workflow
        class SyncCallAssignmentWorkflow(Workflow):
            async def run(self) -> datetime:
                timestamp = datetime.now()
                return timestamp

        with pytest.raises(UnsupportedPatternError) as exc_info:
            SyncCallAssignmentWorkflow.workflow_ir()

        error = cast(UnsupportedPatternError, exc_info.value)
        assert "datetime.now" in error.message
        assert "synchronous function" in error.message.lower()
        assert "@action" in error.recommendation

    def test_sync_function_call_return_raises_error(self) -> None:
        """Test: returning a sync function call raises UnsupportedPatternError."""
        from datetime import datetime
        from typing import cast

        import pytest

        from waymark import workflow
        from waymark.ir_builder import UnsupportedPatternError
        from waymark.workflow import Workflow

        @workflow
        class SyncCallReturnWorkflow(Workflow):
            async def run(self) -> datetime:
                return datetime.now()

        with pytest.raises(UnsupportedPatternError) as exc_info:
            SyncCallReturnWorkflow.workflow_ir()

        error = cast(UnsupportedPatternError, exc_info.value)
        assert "datetime.now" in error.message
        assert "synchronous function" in error.message.lower()
        assert "@action" in error.recommendation

    def test_sync_function_call_in_action_arg_raises_error(self) -> None:
        """Test: sync function calls in action args raise UnsupportedPatternError."""
        from datetime import datetime
        from typing import cast

        import pytest

        from waymark import action, workflow
        from waymark.ir_builder import UnsupportedPatternError
        from waymark.workflow import Workflow

        @action
        async def record_timestamp(timestamp: datetime) -> datetime:
            return timestamp

        @workflow
        class SyncCallActionArgWorkflow(Workflow):
            async def run(self) -> datetime:
                return await record_timestamp(timestamp=datetime.now())

        with pytest.raises(UnsupportedPatternError) as exc_info:
            SyncCallActionArgWorkflow.workflow_ir()

        error = cast(UnsupportedPatternError, exc_info.value)
        assert "datetime.now" in error.message
        assert "synchronous function" in error.message.lower()
        assert "@action" in error.recommendation

    def test_plain_function_call_raises_error(self) -> None:
        """Test: plain function calls in workflow code raise UnsupportedPatternError."""
        from typing import cast

        import pytest

        from waymark import workflow
        from waymark.ir_builder import UnsupportedPatternError
        from waymark.workflow import Workflow

        def log_event(value: int) -> None:
            _ = value

        @workflow
        class PlainFunctionCallWorkflow(Workflow):
            async def run(self) -> int:
                log_event(value=1)
                return 1

        with pytest.raises(UnsupportedPatternError) as exc_info:
            PlainFunctionCallWorkflow.workflow_ir()

        error = cast(UnsupportedPatternError, exc_info.value)
        assert "log_event" in error.message
        assert "synchronous function" in error.message.lower()

    def test_attribute_function_call_raises_error(self) -> None:
        """Test: attribute calls like token.upper() raise UnsupportedPatternError."""
        from typing import cast

        import pytest

        from waymark import workflow
        from waymark.ir_builder import UnsupportedPatternError
        from waymark.workflow import Workflow

        @workflow
        class AttributeFunctionCallWorkflow(Workflow):
            async def run(self) -> str:
                token = "alpha"
                token.upper()
                return token

        with pytest.raises(UnsupportedPatternError) as exc_info:
            AttributeFunctionCallWorkflow.workflow_ir()

        error = cast(UnsupportedPatternError, exc_info.value)
        assert "token.upper" in error.message
        assert "synchronous function" in error.message.lower()


class TestValidPatterns:
    """Test that valid patterns do NOT raise errors."""

    def test_action_return_is_valid(self) -> None:
        """Test: return await action() is valid."""
        from tests.fixtures_actions.action_return import ActionReturnWorkflow

        # Should not raise
        program = ActionReturnWorkflow.workflow_ir()
        assert program is not None

    def test_variable_return_is_valid(self) -> None:
        """Test: return some_var is valid."""
        from tests.fixtures_actions.variable_return import VariableReturnWorkflow

        # Should not raise
        program = VariableReturnWorkflow.workflow_ir()
        assert program is not None

    def test_literal_return_is_valid(self) -> None:
        """Test: return 42 is valid."""
        from tests.fixtures_actions.literal_return import LiteralReturnWorkflow

        # Should not raise
        program = LiteralReturnWorkflow.workflow_ir()
        assert program is not None

    def test_action_call_is_valid(self) -> None:
        """Test: await action() is valid."""
        from tests.fixtures_actions.simple_action import SimpleActionWorkflow

        # Should not raise
        program = SimpleActionWorkflow.workflow_ir()
        assert program is not None

    def test_self_method_call_in_action_arg_is_valid(self) -> None:
        """Test: self.method() inside action args is valid."""
        from waymark import action, workflow
        from waymark.workflow import Workflow

        @action
        async def use_value(value: int) -> int:
            return value

        @workflow
        class HelperInActionArgWorkflow(Workflow):
            def compute(self, base: int) -> int:
                return base + 1

            async def run(self, base: int) -> int:
                return await use_value(value=self.compute(base=base))

        program = HelperInActionArgWorkflow.workflow_ir()
        assert program is not None

    def test_len_global_function_is_allowed(self) -> None:
        """Test: len() is allowed and tagged as a global function."""
        from waymark import workflow
        from waymark.workflow import Workflow

        @workflow
        class LenWorkflow(Workflow):
            async def run(self, items: list[int]) -> int:
                return len(items)

        program = LenWorkflow.workflow_ir()
        assert program is not None

        fn_call = None
        for stmt in program.functions[0].body.statements:
            if stmt.HasField("assignment") and stmt.assignment.value.HasField("function_call"):
                fn_call = stmt.assignment.value.function_call
                break

        assert fn_call is not None, "Expected function_call from len()"
        assert fn_call.global_function == ir.GlobalFunction.GLOBAL_FUNCTION_LEN


class TestIsinstanceToIsexception:
    """Test that isinstance(x, ExceptionClass) is transformed to isexception()."""

    def test_isinstance_single_exception_transforms_to_isexception(self) -> None:
        """Test: isinstance(err, ValueError) transforms to isexception(err, "ValueError")."""
        from waymark import action, workflow
        from waymark.workflow import Workflow

        @action
        async def boom() -> None:
            raise ValueError("boom")

        @workflow
        class W(Workflow):
            async def run(self) -> bool:
                try:
                    await boom()
                    return False
                except Exception as err:
                    return isinstance(err, ValueError)

        program = W.workflow_ir()
        assert program is not None

        # Find the try_except statement
        fn = program.functions[0]
        try_stmt = None
        for stmt in fn.body.statements:
            if stmt.HasField("try_except"):
                try_stmt = stmt.try_except
                break

        assert try_stmt is not None, "Expected try_except statement"
        assert len(try_stmt.handlers) == 1

        handler = try_stmt.handlers[0]
        # The isinstance call may be normalized to an assignment, then returned
        # Look for any function_call named "isexception" in assignments
        fn_call = None
        for stmt in handler.block_body.statements:
            if stmt.HasField("assignment") and stmt.assignment.value.HasField("function_call"):
                if stmt.assignment.value.function_call.name == "isexception":
                    fn_call = stmt.assignment.value.function_call
                    break

        assert fn_call is not None, "Expected isexception function_call"
        assert fn_call.name == "isexception"
        assert fn_call.global_function == ir.GlobalFunction.GLOBAL_FUNCTION_ISEXCEPTION
        assert len(fn_call.args) == 2
        assert fn_call.args[0].variable.name == "err"
        assert fn_call.args[1].literal.string_value == "ValueError"

    def test_isinstance_tuple_of_exceptions_transforms_to_isexception_with_list(self) -> None:
        """Test: isinstance(err, (ValueError, TypeError)) transforms to isexception(err, ["ValueError", "TypeError"])."""
        from waymark import action, workflow
        from waymark.workflow import Workflow

        @action
        async def boom() -> None:
            raise ValueError("boom")

        @workflow
        class W(Workflow):
            async def run(self) -> bool:
                try:
                    await boom()
                    return False
                except Exception as err:
                    return isinstance(err, (ValueError, TypeError))

        program = W.workflow_ir()
        assert program is not None

        # Find the try_except statement
        fn = program.functions[0]
        try_stmt = None
        for stmt in fn.body.statements:
            if stmt.HasField("try_except"):
                try_stmt = stmt.try_except
                break

        assert try_stmt is not None
        handler = try_stmt.handlers[0]
        # The isinstance call may be normalized to an assignment
        fn_call = None
        for stmt in handler.block_body.statements:
            if stmt.HasField("assignment") and stmt.assignment.value.HasField("function_call"):
                if stmt.assignment.value.function_call.name == "isexception":
                    fn_call = stmt.assignment.value.function_call
                    break

        assert fn_call is not None, "Expected isexception function_call"
        assert fn_call.name == "isexception"
        assert fn_call.global_function == ir.GlobalFunction.GLOBAL_FUNCTION_ISEXCEPTION
        assert len(fn_call.args) == 2
        assert fn_call.args[0].variable.name == "err"
        # Second arg should be a list with two string elements
        assert fn_call.args[1].HasField("list")
        elements = list(fn_call.args[1].list.elements)
        assert len(elements) == 2
        assert elements[0].literal.string_value == "ValueError"
        assert elements[1].literal.string_value == "TypeError"

    def test_isinstance_non_exception_raises_error(self) -> None:
        """Test: isinstance(x, str) raises UnsupportedPatternError."""
        import pytest

        from waymark import action, workflow
        from waymark.ir_builder import UnsupportedPatternError
        from waymark.workflow import Workflow

        @action
        async def get_val() -> str:
            return "test"

        @workflow
        class W(Workflow):
            async def run(self) -> bool:
                val = await get_val()
                return isinstance(val, str)

        with pytest.raises(UnsupportedPatternError) as exc_info:
            W.workflow_ir()

        assert "non-exception class 'str'" in str(exc_info.value)

    def test_isinstance_assignment_transforms_correctly(self) -> None:
        """Test: is_value = isinstance(err, ValueError) transforms correctly."""
        from waymark import action, workflow
        from waymark.workflow import Workflow

        @action
        async def boom() -> None:
            raise ValueError("boom")

        @workflow
        class W(Workflow):
            async def run(self) -> bool:
                try:
                    await boom()
                    return False
                except Exception as err:
                    is_value = isinstance(err, ValueError)
                    return is_value

        program = W.workflow_ir()
        assert program is not None

        # Find the assignment in the except handler
        fn = program.functions[0]
        try_stmt = None
        for stmt in fn.body.statements:
            if stmt.HasField("try_except"):
                try_stmt = stmt.try_except
                break

        assert try_stmt is not None
        handler = try_stmt.handlers[0]
        assignment = None
        for stmt in handler.block_body.statements:
            if stmt.HasField("assignment") and "is_value" in list(stmt.assignment.targets):
                assignment = stmt.assignment
                break

        assert assignment is not None, "Expected assignment to is_value"
        assert assignment.value.HasField("function_call")
        fn_call = assignment.value.function_call
        assert fn_call.name == "isexception"
        assert fn_call.global_function == ir.GlobalFunction.GLOBAL_FUNCTION_ISEXCEPTION


class TestPydanticModelSupport:
    """Test that Pydantic models can be instantiated in workflow code."""

    def _find_dict_assignment(self, program: ir.Program) -> tuple[ir.DictExpr, list[str]] | None:
        """Find an assignment with a dict expression.

        Returns tuple of (DictExpr, targets) where targets are assignment variables.
        """
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("assignment"):
                    if stmt.assignment.value.HasField("dict"):
                        return (
                            stmt.assignment.value.dict,
                            list(stmt.assignment.targets),
                        )
        return None

    def _find_dict_return(self, program: ir.Program) -> ir.DictExpr | None:
        """Find a return statement with a dict expression."""
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("return_stmt") and stmt.return_stmt.HasField("value"):
                    value = stmt.return_stmt.value
                    if value.HasField("dict"):
                        return value.dict
        return None

    def _find_return_expr(self, program: ir.Program) -> ir.Expr | None:
        """Find the first return expression in the program."""
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("return_stmt") and stmt.return_stmt.HasField("value"):
                    return stmt.return_stmt.value
        return None

    def _find_assignment_value(self, program: ir.Program, target: str) -> ir.Expr | None:
        """Find an assignment value by target variable name."""
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("assignment"):
                    if target in list(stmt.assignment.targets):
                        return stmt.assignment.value
        return None

    def _assert_list_elements_are_dicts(self, list_expr: ir.ListExpr) -> None:
        assert list_expr.elements, "Expected list to have elements"
        for element in list_expr.elements:
            assert element.HasField("dict"), (
                f"Expected list element to be dict, got {element.WhichOneof('kind')}"
            )

    def _assert_dict_values_are_dicts(self, dict_expr: ir.DictExpr) -> None:
        assert dict_expr.entries, "Expected dict to have entries"
        for entry in dict_expr.entries:
            assert entry.value.HasField("dict"), (
                f"Expected dict value to be dict, got {entry.value.WhichOneof('kind')}"
            )

    def _get_dict_keys(self, dict_expr: ir.DictExpr) -> list[str]:
        """Extract string keys from a dict expression."""
        keys = []
        for entry in dict_expr.entries:
            if entry.key.HasField("literal") and entry.key.literal.HasField("string_value"):
                keys.append(entry.key.literal.string_value)
        return keys

    def _get_dict_entry_value(self, dict_expr: ir.DictExpr, key: str) -> ir.Expr | None:
        """Get the value for a specific key in a dict expression."""
        for entry in dict_expr.entries:
            if (
                entry.key.HasField("literal")
                and entry.key.literal.HasField("string_value")
                and entry.key.literal.string_value == key
            ):
                return entry.value
        return None

    def test_pydantic_simple_model(self) -> None:
        """Test: Simple Pydantic model is converted to dict expression."""
        from tests.fixtures_models.pydantic_simple import PydanticSimpleWorkflow

        # Should not raise - Pydantic models are allowed
        program = PydanticSimpleWorkflow.workflow_ir()
        assert program is not None

        # Find the dict assignment (result = SimpleResult(...))
        result = self._find_dict_assignment(program)
        assert result is not None, "Expected dict assignment from Pydantic model"
        dict_expr, targets = result

        # Check targets
        assert targets == ["result"], f"Expected target 'result', got {targets}"

        # Check dict has expected keys
        keys = self._get_dict_keys(dict_expr)
        assert "value" in keys, "Expected 'value' key in dict"
        assert "message" in keys, "Expected 'message' key in dict"

    def test_pydantic_return_direct(self) -> None:
        """Test: returning a Pydantic model constructor is converted to dict."""
        from tests.fixtures_models.pydantic_return_direct import PydanticReturnDirectWorkflow

        program = PydanticReturnDirectWorkflow.workflow_ir()
        assert program is not None

        dict_expr = self._find_dict_return(program)
        assert dict_expr is not None, "Expected dict return from Pydantic model"

        keys = self._get_dict_keys(dict_expr)
        assert "value" in keys, "Expected 'value' key in dict"
        assert "message" in keys, "Expected 'message' key in dict"

    def test_pydantic_return_list(self) -> None:
        """Test: returning list of Pydantic models converts elements to dicts."""
        from tests.fixtures_models.pydantic_ast_variants import PydanticReturnListWorkflow

        program = PydanticReturnListWorkflow.workflow_ir()
        assert program is not None

        expr = self._find_return_expr(program)
        assert expr is not None, "Expected return expression"
        assert expr.HasField("list"), f"Expected list return, got {expr.WhichOneof('kind')}"
        self._assert_list_elements_are_dicts(expr.list)

    def test_pydantic_return_dict(self) -> None:
        """Test: returning dict of Pydantic models converts values to dicts."""
        from tests.fixtures_models.pydantic_ast_variants import PydanticReturnDictWorkflow

        program = PydanticReturnDictWorkflow.workflow_ir()
        assert program is not None

        expr = self._find_return_expr(program)
        assert expr is not None, "Expected return expression"
        assert expr.HasField("dict"), f"Expected dict return, got {expr.WhichOneof('kind')}"
        self._assert_dict_values_are_dicts(expr.dict)

    def test_pydantic_assignment_list(self) -> None:
        """Test: assigning list of Pydantic models converts elements to dicts."""
        from tests.fixtures_models.pydantic_ast_variants import PydanticAssignmentListWorkflow

        program = PydanticAssignmentListWorkflow.workflow_ir()
        assert program is not None

        value = self._find_assignment_value(program, "items")
        assert value is not None, "Expected assignment for items"
        assert value.HasField("list"), f"Expected list assignment, got {value.WhichOneof('kind')}"
        self._assert_list_elements_are_dicts(value.list)

    def test_pydantic_assignment_dict(self) -> None:
        """Test: assigning dict of Pydantic models converts values to dicts."""
        from tests.fixtures_models.pydantic_ast_variants import PydanticAssignmentDictWorkflow

        program = PydanticAssignmentDictWorkflow.workflow_ir()
        assert program is not None

        value = self._find_assignment_value(program, "payload")
        assert value is not None, "Expected assignment for payload"
        assert value.HasField("dict"), f"Expected dict assignment, got {value.WhichOneof('kind')}"
        self._assert_dict_values_are_dicts(value.dict)

    def test_pydantic_assignment_tuple(self) -> None:
        """Test: assigning tuple of Pydantic models converts elements to dicts."""
        from tests.fixtures_models.pydantic_ast_variants import PydanticAssignmentTupleWorkflow

        program = PydanticAssignmentTupleWorkflow.workflow_ir()
        assert program is not None

        value = self._find_assignment_value(program, "pair")
        assert value is not None, "Expected assignment for pair"
        assert value.HasField("list"), (
            f"Expected tuple to become list, got {value.WhichOneof('kind')}"
        )
        self._assert_list_elements_are_dicts(value.list)

    def test_pydantic_model_with_defaults(self) -> None:
        """Test: Pydantic model with defaults includes default values in dict."""
        from tests.fixtures_models.pydantic_with_defaults import PydanticDefaultsWorkflow

        program = PydanticDefaultsWorkflow.workflow_ir()
        assert program is not None

        result = self._find_dict_assignment(program)
        assert result is not None, "Expected dict assignment"
        dict_expr, _targets = result

        keys = self._get_dict_keys(dict_expr)
        # 'value' is provided, 'status' and 'count' have defaults
        assert "value" in keys, "Expected 'value' key"
        assert "status" in keys, "Expected 'status' key from default"
        assert "count" in keys, "Expected 'count' key from default"

        # Check default values are present
        status_val = self._get_dict_entry_value(dict_expr, "status")
        assert status_val is not None
        if status_val.HasField("literal"):
            assert status_val.literal.string_value == "ok", "Expected default status='ok'"

        count_val = self._get_dict_entry_value(dict_expr, "count")
        assert count_val is not None
        if count_val.HasField("literal"):
            assert count_val.literal.int_value == 0, "Expected default count=0"

    def test_pydantic_nested_model(self) -> None:
        """Test: Pydantic model with nested dict field."""
        from tests.fixtures_models.pydantic_nested import PydanticNestedWorkflow

        program = PydanticNestedWorkflow.workflow_ir()
        assert program is not None

        result = self._find_dict_assignment(program)
        assert result is not None, "Expected dict assignment"
        dict_expr, _targets = result

        keys = self._get_dict_keys(dict_expr)
        assert "name" in keys, "Expected 'name' key"
        assert "inner" in keys, "Expected 'inner' key"


class TestDataclassSupport:
    """Test that dataclasses can be instantiated in workflow code."""

    def _find_dict_assignment(self, program: ir.Program) -> tuple[ir.DictExpr, list[str]] | None:
        """Find an assignment with a dict expression."""
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("assignment"):
                    if stmt.assignment.value.HasField("dict"):
                        return (
                            stmt.assignment.value.dict,
                            list(stmt.assignment.targets),
                        )
        return None

    def _get_dict_keys(self, dict_expr: ir.DictExpr) -> list[str]:
        """Extract string keys from a dict expression."""
        keys = []
        for entry in dict_expr.entries:
            if entry.key.HasField("literal") and entry.key.literal.HasField("string_value"):
                keys.append(entry.key.literal.string_value)
        return keys

    def _get_dict_entry_value(self, dict_expr: ir.DictExpr, key: str) -> ir.Expr | None:
        """Get the value for a specific key in a dict expression."""
        for entry in dict_expr.entries:
            if (
                entry.key.HasField("literal")
                and entry.key.literal.HasField("string_value")
                and entry.key.literal.string_value == key
            ):
                return entry.value
        return None

    def test_dataclass_simple(self) -> None:
        """Test: Simple dataclass is converted to dict expression."""
        from tests.fixtures_models.dataclass_simple import DataclassSimpleWorkflow

        program = DataclassSimpleWorkflow.workflow_ir()
        assert program is not None

        result = self._find_dict_assignment(program)
        assert result is not None, "Expected dict assignment from dataclass"
        dict_expr, targets = result

        assert targets == ["result"], f"Expected target 'result', got {targets}"

        keys = self._get_dict_keys(dict_expr)
        assert "value" in keys, "Expected 'value' key"
        assert "message" in keys, "Expected 'message' key"

    def test_dataclass_with_defaults(self) -> None:
        """Test: Dataclass with defaults includes default values in dict."""
        from tests.fixtures_models.dataclass_with_defaults import DataclassDefaultsWorkflow

        program = DataclassDefaultsWorkflow.workflow_ir()
        assert program is not None

        result = self._find_dict_assignment(program)
        assert result is not None, "Expected dict assignment"
        dict_expr, _targets = result

        keys = self._get_dict_keys(dict_expr)
        assert "value" in keys, "Expected 'value' key"
        assert "status" in keys, "Expected 'status' key from default"
        assert "retry_count" in keys, "Expected 'retry_count' key from default"

        # Check default values
        status_val = self._get_dict_entry_value(dict_expr, "status")
        assert status_val is not None
        if status_val.HasField("literal"):
            assert status_val.literal.string_value == "pending"

        retry_val = self._get_dict_entry_value(dict_expr, "retry_count")
        assert retry_val is not None
        if retry_val.HasField("literal"):
            assert retry_val.literal.int_value == 0

    def test_dataclass_positional_args(self) -> None:
        """Test: Dataclass with positional arguments."""
        from tests.fixtures_models.dataclass_positional import DataclassPositionalWorkflow

        program = DataclassPositionalWorkflow.workflow_ir()
        assert program is not None

        result = self._find_dict_assignment(program)
        assert result is not None, "Expected dict assignment"
        dict_expr, targets = result

        assert targets == ["point"], f"Expected target 'point', got {targets}"

        keys = self._get_dict_keys(dict_expr)
        assert "x" in keys, "Expected 'x' key from positional arg"
        assert "y" in keys, "Expected 'y' key from positional arg"


class TestPydanticModelAsActionArgument:
    """Test that Pydantic models passed directly as action arguments are converted to dicts.

    This tests the case where a Pydantic model constructor is passed directly
    as an argument to an action call:

        await self.run_action(my_action(MyModel(field=value)))

    The model constructor should be converted to a dict expression in the IR,
    not left as a FunctionCall (which Rust can't evaluate).
    """

    def _find_action_call_kwargs(
        self, program: ir.Program, action_name: str
    ) -> list[ir.Kwarg] | None:
        """Find kwargs for an action call in the program.

        Checks both:
        - Direct action_call statements
        - Assignment statements where value contains an action_call expression
        """
        for fn in program.functions:
            for stmt in fn.body.statements:
                # Check direct action_call statement
                if stmt.HasField("action_call"):
                    if stmt.action_call.action_name == action_name:
                        return list(stmt.action_call.kwargs)
                # Check assignment with action_call expression
                if stmt.HasField("assignment"):
                    if stmt.assignment.value.HasField("action_call"):
                        action = stmt.assignment.value.action_call
                        if action.action_name == action_name:
                            return list(action.kwargs)
        return None

    def test_pydantic_model_as_action_arg_is_dict(self) -> None:
        """Test: Pydantic model passed as action argument should become dict in IR."""
        from tests.fixtures_models.pydantic_action_arg import PydanticActionArgWorkflow

        # Should not raise - should compile the IR
        program = PydanticActionArgWorkflow.workflow_ir()
        assert program is not None

        # Find the action call to process_request
        kwargs = self._find_action_call_kwargs(program, "process_request")
        assert kwargs is not None, "Expected to find process_request action call"
        assert len(kwargs) == 1, f"Expected 1 kwarg (request), got {len(kwargs)}"

        # The kwarg should be a dict expression (converted from Pydantic model)
        # NOT a function_call expression
        request_kwarg = kwargs[0]
        assert request_kwarg.name == "request", (
            f"Expected 'request' kwarg, got {request_kwarg.name}"
        )

        # The value should be a dict expression, not a function call
        value = request_kwarg.value
        assert value is not None, "Expected kwarg to have a value"

        # This is the key assertion: the model constructor should be converted to a dict
        # If it's still a function_call, the fix is needed
        assert value.HasField("dict"), (
            f"Expected Pydantic model constructor to be converted to dict expression, "
            f"but got: {value.WhichOneof('kind')}"
        )


class TestDataclassAsActionArgument:
    """Test that dataclasses passed directly as action arguments are converted to dicts.

    This tests the case where a dataclass constructor is passed directly
    as an argument to an action call:

        await self.run_action(my_action(MyDataclass(field=value)))

    The dataclass constructor should be converted to a dict expression in the IR,
    not left as a FunctionCall (which Rust can't evaluate).
    """

    def _find_action_call_kwargs(
        self, program: ir.Program, action_name: str
    ) -> list[ir.Kwarg] | None:
        """Find kwargs for an action call in the program."""
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("action_call"):
                    if stmt.action_call.action_name == action_name:
                        return list(stmt.action_call.kwargs)
                if stmt.HasField("assignment"):
                    if stmt.assignment.value.HasField("action_call"):
                        action = stmt.assignment.value.action_call
                        if action.action_name == action_name:
                            return list(action.kwargs)
        return None

    def test_dataclass_as_action_arg_is_dict(self) -> None:
        """Test: Dataclass passed as action argument should become dict in IR."""
        from tests.fixtures_models.dataclass_action_arg import DataclassActionArgWorkflow

        program = DataclassActionArgWorkflow.workflow_ir()
        assert program is not None

        kwargs = self._find_action_call_kwargs(program, "process_data")
        assert kwargs is not None, "Expected to find process_data action call"
        assert len(kwargs) == 1, f"Expected 1 kwarg (request), got {len(kwargs)}"

        request_kwarg = kwargs[0]
        assert request_kwarg.name == "request", (
            f"Expected 'request' kwarg, got {request_kwarg.name}"
        )

        value = request_kwarg.value
        assert value is not None, "Expected kwarg to have a value"

        # Key assertion: dataclass constructor should be converted to dict
        assert value.HasField("dict"), (
            f"Expected dataclass constructor to be converted to dict expression, "
            f"but got: {value.WhichOneof('kind')}"
        )
