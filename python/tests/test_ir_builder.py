"""Tests for IR builder functionality.

This module contains comprehensive tests for the Python AST to IR conversion.
The tests are organized by category:
- TestAsyncioSleepDetection: asyncio.sleep() -> @sleep action
- TestAsyncioGatherDetection: asyncio.gather() -> ParallelBlock/SpreadAction
- TestPolicyParsing: RetryPolicy and TimeoutPolicy extraction
- TestForLoopConversion: for loop IR generation
- TestConditionalConversion: if/elif/else IR generation
- TestTryExceptConversion: try/except IR generation
- TestActionCallExtraction: action call detection and kwargs
- TestWorkflowHelperMethods: self.method() -> FunctionCall
"""

from typing import List, Optional

from proto import ast_pb2 as ir

# Global variable for test_global_statement_raises_error test
some_var: int = 0


class TestAsyncioSleepDetection:
    """Test that asyncio.sleep is detected and converted to @sleep action."""

    def _find_sleep_action(self, program: ir.Program) -> ir.ActionCall | None:
        """Find a @sleep action call in the program."""
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("action_call"):
                    if stmt.action_call.action_name == "sleep":
                        return stmt.action_call
        return None

    def _get_duration_kwarg(self, action_call: ir.ActionCall) -> ir.Kwarg | None:
        """Get the duration kwarg from a sleep action."""
        for kw in action_call.kwargs:
            if kw.name == "duration":
                return kw
        return None

    def test_asyncio_dot_sleep_pattern(self) -> None:
        """Test: import asyncio; asyncio.sleep(1)"""
        from tests.fixtures_sleep.sleep_import_asyncio import SleepImportAsyncioWorkflow

        program = SleepImportAsyncioWorkflow.workflow_ir()

        sleep_action = self._find_sleep_action(program)
        assert sleep_action is not None, "Expected @sleep action in IR"

        duration = self._get_duration_kwarg(sleep_action)
        assert duration is not None, "Expected duration kwarg"
        assert duration.value.HasField("literal"), "Expected literal value"
        assert duration.value.literal.int_value == 1

    def test_from_asyncio_import_sleep_pattern(self) -> None:
        """Test: from asyncio import sleep; sleep(2)"""
        from tests.fixtures_sleep.sleep_from_import import SleepFromImportWorkflow

        program = SleepFromImportWorkflow.workflow_ir()

        sleep_action = self._find_sleep_action(program)
        assert sleep_action is not None, "Expected @sleep action in IR"

        duration = self._get_duration_kwarg(sleep_action)
        assert duration is not None, "Expected duration kwarg"
        assert duration.value.HasField("literal"), "Expected literal value"
        assert duration.value.literal.int_value == 2

    def test_from_asyncio_import_sleep_as_alias_pattern(self) -> None:
        """Test: from asyncio import sleep as async_sleep; async_sleep(3)"""
        from tests.fixtures_sleep.sleep_aliased_import import SleepAliasedImportWorkflow

        program = SleepAliasedImportWorkflow.workflow_ir()

        sleep_action = self._find_sleep_action(program)
        assert sleep_action is not None, "Expected @sleep action in IR"

        duration = self._get_duration_kwarg(sleep_action)
        assert duration is not None, "Expected duration kwarg"
        assert duration.value.HasField("literal"), "Expected literal value"
        assert duration.value.literal.int_value == 3


class TestPolicyParsing:
    """Test that retry and timeout policies are parsed from run_action calls."""

    def _find_action_with_policies(
        self, program: ir.Program, action_name: str
    ) -> ir.ActionCall | None:
        """Find an action call by name, searching in all contexts."""
        for fn in program.functions:
            for stmt in fn.body.statements:
                # Direct action call (statement form)
                if stmt.HasField("action_call"):
                    if stmt.action_call.action_name == action_name:
                        return stmt.action_call
                # Action call in assignment (expression form)
                elif stmt.HasField("assignment"):
                    if stmt.assignment.value.HasField("action_call"):
                        if stmt.assignment.value.action_call.action_name == action_name:
                            return stmt.assignment.value.action_call
                # Action in try body
                elif stmt.HasField("try_except"):
                    te = stmt.try_except
                    if te.try_body.HasField("call") and te.try_body.call.HasField("action"):
                        if te.try_body.call.action.action_name == action_name:
                            return te.try_body.call.action
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
        assert policy.retry.max_retries == 1

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
        """Test: a, b = await asyncio.gather(action_a(), action_b())"""
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
        """Test: results = await asyncio.gather(a(), b(), c())"""
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
        """Test: await asyncio.gather(*[action(x) for x in items])

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
        """Test: a, b = await asyncio.gather(action1(), action2())

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

    def _find_implicit_function(self, program: ir.Program, prefix: str) -> ir.FunctionDef | None:
        """Find an implicit function by name prefix."""
        for fn in program.functions:
            if fn.name.startswith(prefix):
                return fn
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

        # Body should have a call
        assert for_loop.body.HasField("call"), "Expected call in for loop body"

    def test_multi_action_for_creates_implicit_function(self) -> None:
        """Test: Multi-action for loop body is wrapped in implicit function."""
        from tests.fixtures_control_flow.for_multi_action import ForMultiActionWorkflow

        program = ForMultiActionWorkflow.workflow_ir()

        # Should have an implicit function for the multi-action body
        implicit_fn = self._find_implicit_function(program, "__for_body")
        assert implicit_fn is not None, "Expected implicit function for multi-action for body"

        # The implicit function should have multiple statements
        assert len(implicit_fn.body.statements) >= 2, (
            "Implicit function should have multiple statements"
        )


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
        """Test: Single list.append() is detected as accumulator target."""
        from tests.fixtures_for_loop.for_single_accumulator import ForSingleAccumulatorWorkflow

        program = ForSingleAccumulatorWorkflow.workflow_ir()
        for_loop = self._find_for_loop(program)
        assert for_loop is not None, "Expected for_loop in IR"

        # The body should have 'results' as target (from results.append())
        assert "results" in for_loop.body.targets, (
            f"Expected 'results' in targets, got: {list(for_loop.body.targets)}"
        )

    def test_multi_list_append_in_conditionals(self) -> None:
        """Test: Accumulators in conditional branches are detected but don't override action targets.

        When the for loop body has both an action call with targets AND conditional
        appends, the action targets take precedence. This is because the spread pattern
        uses the action targets to know what the action returns per-iteration.

        The conditional accumulation pattern (different lists based on condition) requires
        different handling than simple accumulation.
        """
        from tests.fixtures_for_loop.for_multi_accumulator import ForMultiAccumulatorWorkflow

        program = ForMultiAccumulatorWorkflow.workflow_ir()
        for_loop = self._find_for_loop(program)
        assert for_loop is not None, "Expected for_loop in IR"

        # The action call targets should be present (is_valid, processed from tuple unpacking)
        targets = list(for_loop.body.targets)
        assert "is_valid" in targets or "processed" in targets, (
            f"Expected action targets in body, got: {targets}"
        )

    def test_for_with_append_original_fixture(self) -> None:
        """Test: Original for_with_append fixture works correctly."""
        from tests.fixtures_for_loop.for_with_append import ForWithAppendWorkflow

        program = ForWithAppendWorkflow.workflow_ir()
        for_loop = self._find_for_loop(program)
        assert for_loop is not None, "Expected for_loop in IR"

        # The body should have 'results' as target
        assert "results" in for_loop.body.targets, (
            f"Expected 'results' in targets, got: {list(for_loop.body.targets)}"
        )

    def test_loop_variable_not_detected_as_accumulator(self) -> None:
        """Test: Loop variables are not incorrectly detected as accumulators."""
        from tests.fixtures_for_loop.for_single_accumulator import ForSingleAccumulatorWorkflow

        program = ForSingleAccumulatorWorkflow.workflow_ir()
        for_loop = self._find_for_loop(program)
        assert for_loop is not None, "Expected for_loop in IR"

        # 'item' is the loop variable, should NOT be in targets
        assert "item" not in for_loop.body.targets, (
            f"Loop variable 'item' should not be in targets, got: {list(for_loop.body.targets)}"
        )

    def test_in_scope_variable_not_detected_as_accumulator(self) -> None:
        """Test: Variables defined in loop body are not detected as accumulators."""
        from tests.fixtures_for_loop.for_single_accumulator import ForSingleAccumulatorWorkflow

        program = ForSingleAccumulatorWorkflow.workflow_ir()
        for_loop = self._find_for_loop(program)
        assert for_loop is not None, "Expected for_loop in IR"

        # 'processed' is defined in the loop body, should NOT be in targets
        assert "processed" not in for_loop.body.targets, (
            f"In-scope variable 'processed' should not be in targets, got: {list(for_loop.body.targets)}"
        )

    def test_accumulator_with_wrapped_body(self) -> None:
        """Test: Accumulators are detected even when body is wrapped in function."""
        from tests.fixtures_for_loop.for_with_append import ForWithAppendWorkflow

        program = ForWithAppendWorkflow.workflow_ir()
        for_loop = self._find_for_loop(program)
        assert for_loop is not None, "Expected for_loop in IR"

        # Should have an implicit function (multi-action body)
        has_implicit_fn = any(fn.name.startswith("__for_body") for fn in program.functions)
        assert has_implicit_fn, "Expected implicit function for multi-action body"

        # Even with wrapped body, 'results' should be detected
        assert "results" in for_loop.body.targets, (
            f"Expected 'results' in targets even with wrapped body, got: {list(for_loop.body.targets)}"
        )


class TestConditionalAccumulatorDetection:
    """Test detection of accumulator patterns in conditionals."""

    def _find_implicit_function(self, program: ir.Program, prefix: str) -> ir.FunctionDef | None:
        """Find an implicit function by name prefix."""
        for fn in program.functions:
            if fn.name.startswith(prefix):
                return fn
        return None

    def test_if_with_multi_action_body_and_accumulator(self) -> None:
        """Test: Conditional with multi-action body detects modified variables."""
        from tests.fixtures_control_flow.if_with_accumulator import IfWithAccumulatorWorkflow

        program = IfWithAccumulatorWorkflow.workflow_ir()

        # Should have an implicit function for the multi-action if branch
        implicit_fn = self._find_implicit_function(program, "__if_then")
        assert implicit_fn is not None, "Expected implicit function for multi-action if body"

        # The implicit function should have 'results' as both input and output
        assert "results" in implicit_fn.io.inputs, (
            f"Expected 'results' in function inputs, got: {list(implicit_fn.io.inputs)}"
        )
        assert "results" in implicit_fn.io.outputs, (
            f"Expected 'results' in function outputs, got: {list(implicit_fn.io.outputs)}"
        )

        # Should have a return statement
        has_return = any(stmt.HasField("return_stmt") for stmt in implicit_fn.body.statements)
        assert has_return, "Expected return statement in implicit function"


class TestConditionalConversion:
    """Test if/elif/else conversion to IR."""

    def _find_conditional(self, program: ir.Program) -> ir.Conditional | None:
        """Find a conditional in the program."""
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("conditional"):
                    return stmt.conditional
        return None

    def _find_implicit_function(self, program: ir.Program, prefix: str) -> ir.FunctionDef | None:
        """Find an implicit function by name prefix."""
        for fn in program.functions:
            if fn.name.startswith(prefix):
                return fn
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
        assert conditional.if_branch.HasField("body"), "Expected if_branch body"

        # Should have else_branch
        assert conditional.HasField("else_branch"), "Expected else_branch"

    def test_elif_chain_creates_branches(self) -> None:
        """Test: if/elif/elif/else creates proper branch structure."""
        from tests.fixtures_control_flow.if_elif_else import IfElifElseWorkflow

        program = IfElifElseWorkflow.workflow_ir()

        conditional = self._find_conditional(program)
        assert conditional is not None, "Expected conditional in IR"

        # Should have elif branches
        assert len(conditional.elif_branches) >= 2, "Expected at least 2 elif branches"

    def test_multi_action_branches_create_implicit_functions(self) -> None:
        """Test: Multi-action if/else branches are wrapped in implicit functions."""
        from tests.fixtures_control_flow.if_multi_action import IfMultiActionWorkflow

        program = IfMultiActionWorkflow.workflow_ir()

        # Should have implicit functions for the branches
        if_fn = self._find_implicit_function(program, "__if_then")
        else_fn = self._find_implicit_function(program, "__if_else")

        # At least one should exist (both branches have multi-action)
        assert if_fn is not None or else_fn is not None, (
            "Expected implicit function for multi-action branches"
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

    def _find_implicit_function(self, program: ir.Program, prefix: str) -> ir.FunctionDef | None:
        """Find an implicit function by name prefix."""
        for fn in program.functions:
            if fn.name.startswith(prefix):
                return fn
        return None

    def test_simple_try_except_structure(self) -> None:
        """Test: Simple try/except has correct structure."""
        from tests.fixtures_control_flow.try_simple import TrySimpleWorkflow

        program = TrySimpleWorkflow.workflow_ir()

        try_except = self._find_try_except(program)
        assert try_except is not None, "Expected try_except in IR"

        # Should have try body
        assert try_except.HasField("try_body"), "Expected try_body"

        # Should have at least one handler
        assert len(try_except.handlers) >= 1, "Expected at least one exception handler"

    def test_multi_action_try_creates_implicit_function(self) -> None:
        """Test: Multi-action try body is wrapped in implicit function."""
        from tests.fixtures_control_flow.try_multi_action import TryMultiActionWorkflow

        program = TryMultiActionWorkflow.workflow_ir()

        # Should have an implicit function for the multi-action try body
        implicit_fn = self._find_implicit_function(program, "__try_body")
        assert implicit_fn is not None, "Expected implicit function for multi-action try body"

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

        # Should find self.compute_multiplier as a function call
        fc = self._find_function_call_in_assignments(program, "self.compute_multiplier")
        assert fc is not None, "Expected self.compute_multiplier as FunctionCall"

        # Should find self.format_result as a function call
        fc2 = self._find_function_call_in_assignments(program, "self.format_result")
        assert fc2 is not None, "Expected self.format_result as FunctionCall"

    def test_helper_method_kwargs_preserved(self) -> None:
        """Test: self.method(a=x, b=y) preserves keyword arguments."""
        from tests.fixtures_workflow.workflow_helper_methods import WorkflowWithHelperMethods

        program = WorkflowWithHelperMethods.workflow_ir()

        fc = self._find_function_call_in_assignments(program, "self.compute_multiplier")
        assert fc is not None, "Expected self.compute_multiplier"

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

        fc = self._find_function_call_in_assignments(program, "self.format_result")
        assert fc is not None, "Expected self.format_result"

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

        fc = self._find_function_call_in_assignments(program, "self.add")
        assert fc is not None, "Expected self.add"

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

        fc = self._find_function_call_in_assignments(program, "self.multiply")
        assert fc is not None, "Expected self.multiply"

        # Should have 2 positional args and 1 kwarg
        assert len(fc.args) == 2, f"Expected 2 positional args, got {len(fc.args)}"
        assert len(fc.kwargs) == 1, f"Expected 1 kwarg, got {len(fc.kwargs)}"

        # Positional args
        assert fc.args[0].variable.name == "sum_result", "Expected reference to 'sum_result'"
        assert fc.args[1].literal.int_value == 2, "Expected literal 2"

        # Keyword arg
        assert fc.kwargs[0].name == "z", "Expected 'z' kwarg"
        assert fc.kwargs[0].value.literal.int_value == 3, "Expected z=3"


class TestUnsupportedPatternDetection:
    """Test that unsupported patterns raise UnsupportedPatternError with recommendations."""

    def test_gather_variable_spread_raises_error(self) -> None:
        """Test: asyncio.gather(*tasks) raises error with recommendation."""
        import pytest

        from rappel import UnsupportedPatternError
        from tests.fixtures_gather.gather_unsupported_variable import (
            GatherUnsupportedVariableWorkflow,
        )

        with pytest.raises(UnsupportedPatternError) as exc_info:
            GatherUnsupportedVariableWorkflow.workflow_ir()

        error = exc_info.value
        assert isinstance(error, UnsupportedPatternError)
        assert "tasks" in error.message, "Error should mention the variable name"
        assert "gather" in error.message.lower(), "Error should mention gather"
        assert "list comprehension" in error.recommendation.lower(), (
            "Recommendation should suggest list comprehension"
        )

    def test_fstring_raises_error(self) -> None:
        """Test: f-strings raise error with recommendation."""
        import pytest

        from rappel import UnsupportedPatternError, action, workflow
        from rappel.workflow import Workflow

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

    def test_while_loop_raises_error(self) -> None:
        """Test: while loops raise error with recommendation."""
        import pytest

        from rappel import UnsupportedPatternError, action, workflow
        from rappel.workflow import Workflow

        @action(name="while_test_action")
        async def while_action() -> int:
            return 1

        @workflow
        class WhileWorkflow(Workflow):
            async def run(self, count: int) -> int:
                i = 0
                while i < count:
                    await while_action()
                    i += 1
                return i

        with pytest.raises(UnsupportedPatternError) as exc_info:
            WhileWorkflow.workflow_ir()

        error = exc_info.value
        assert isinstance(error, UnsupportedPatternError)
        assert "While" in error.message, "Error should mention while loops"
        assert "for loop" in error.recommendation.lower(), "Recommendation should suggest for loop"

    def test_with_statement_raises_error(self) -> None:
        """Test: with statements raise error with recommendation."""
        import pytest

        from rappel import UnsupportedPatternError, workflow
        from rappel.workflow import Workflow

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

        from rappel import UnsupportedPatternError, workflow
        from rappel.workflow import Workflow

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

    def test_list_comprehension_outside_gather_raises_error(self) -> None:
        """Test: list comprehensions outside gather context raise error."""
        import pytest

        from rappel import UnsupportedPatternError, workflow
        from rappel.workflow import Workflow

        @workflow
        class ListCompWorkflow(Workflow):
            async def run(self, items: list) -> list:
                doubled = [x * 2 for x in items]
                return doubled

        with pytest.raises(UnsupportedPatternError) as exc_info:
            ListCompWorkflow.workflow_ir()

        error = exc_info.value
        assert isinstance(error, UnsupportedPatternError)
        assert "List comprehension" in error.message, "Error should mention list comprehensions"
        assert "asyncio.gather" in error.recommendation, (
            "Recommendation should mention gather context"
        )

    def test_delete_statement_raises_error(self) -> None:
        """Test: del statements raise error with recommendation."""
        import pytest

        from rappel import UnsupportedPatternError, workflow
        from rappel.workflow import Workflow

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

        from rappel import UnsupportedPatternError
        from tests.fixtures_gather.gather_unsupported_variable import (
            GatherUnsupportedVariableWorkflow,
        )

        with pytest.raises(UnsupportedPatternError) as exc_info:
            GatherUnsupportedVariableWorkflow.workflow_ir()

        error = exc_info.value
        assert isinstance(error, UnsupportedPatternError)
        assert error.line is not None, "Error should include line number"
        assert error.line > 0, "Line number should be positive"

    def test_global_statement_raises_error(self) -> None:
        """Test: global statements raise error."""
        import pytest

        from rappel import UnsupportedPatternError, action, workflow
        from rappel.workflow import Workflow

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

        from rappel import UnsupportedPatternError, workflow
        from rappel.workflow import Workflow

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

        from rappel import UnsupportedPatternError, workflow
        from rappel.workflow import Workflow

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

        from rappel import UnsupportedPatternError, workflow
        from rappel.workflow import Workflow

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

        from rappel import UnsupportedPatternError, workflow
        from rappel.workflow import Workflow

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


class TestReturnStatements:
    """Test return statement handling."""

    def test_return_with_variable(self) -> None:
        """Test: return with a variable reference."""
        from rappel import action, workflow
        from rappel.workflow import Workflow

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
        from rappel import workflow
        from rappel.workflow import Workflow

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
        from rappel import workflow
        from rappel.workflow import Workflow

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
        from rappel import action, workflow
        from rappel.workflow import Workflow

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
        from rappel import workflow
        from rappel.workflow import Workflow

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
        from rappel import workflow
        from rappel.workflow import Workflow

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
        from rappel import workflow
        from rappel.workflow import Workflow

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
        from rappel import workflow
        from rappel.workflow import Workflow

        @workflow
        class DotWorkflow(Workflow):
            async def run(self, obj: dict) -> str:
                # Use a simple attribute access pattern
                name = obj.get
                return str(name)

        program = DotWorkflow.workflow_ir()
        # Just verify it builds without error
        assert program is not None

    def test_unary_not_expression(self) -> None:
        """Test: not x unary operator."""
        from rappel import workflow
        from rappel.workflow import Workflow

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
        from rappel import workflow
        from rappel.workflow import Workflow

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
        from rappel import workflow
        from rappel.workflow import Workflow

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
        from rappel import workflow
        from rappel.workflow import Workflow

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
        from rappel import workflow
        from rappel.workflow import Workflow

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
        from rappel import workflow
        from rappel.workflow import Workflow

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
        from rappel import workflow
        from rappel.workflow import Workflow

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
        from rappel import workflow
        from rappel.workflow import Workflow

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

        from rappel import UnsupportedPatternError, workflow
        from rappel.workflow import Workflow

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

        from rappel import UnsupportedPatternError, workflow
        from rappel.workflow import Workflow

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

        from rappel import UnsupportedPatternError, workflow
        from rappel.workflow import Workflow

        @workflow
        class GeneratorWorkflow(Workflow):
            async def run(self, items: list[int]) -> int:
                return sum(x * 2 for x in items)

        with pytest.raises(UnsupportedPatternError) as exc_info:
            GeneratorWorkflow.workflow_ir()

        error = exc_info.value
        assert isinstance(error, UnsupportedPatternError)
        assert "Generator" in error.message

    def test_walrus_operator_raises_error(self) -> None:
        """Test: walrus operator raises error."""
        import pytest

        from rappel import UnsupportedPatternError, workflow
        from rappel.workflow import Workflow

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

        from rappel import UnsupportedPatternError

        # Use fixture file to test match statement (requires source code access)
        from tests.fixtures_unsupported.match_workflow import MatchWorkflow

        with pytest.raises(UnsupportedPatternError) as exc_info:
            MatchWorkflow.workflow_ir()

        error = exc_info.value
        assert isinstance(error, UnsupportedPatternError)
        assert "Match" in error.message


class TestForLoopEnumerate:
    """Test for loop with enumerate pattern."""

    def test_for_enumerate_unpacking(self) -> None:
        """Test: for i, item in enumerate(items) creates correct loop vars."""
        from rappel import action, workflow
        from rappel.workflow import Workflow

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

        # Find the for loop
        func = program.functions[0]
        for_loop = None
        for stmt in func.body.statements:
            if stmt.HasField("for_loop"):
                for_loop = stmt.for_loop
                break

        assert for_loop is not None, "Should have for loop"
        # enumerate unpacks to two loop vars
        assert len(for_loop.loop_vars) == 2, "Should have 2 loop vars (i, item)"
        assert "i" in for_loop.loop_vars, "Should have 'i' loop var"
        assert "item" in for_loop.loop_vars, "Should have 'item' loop var"


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
        from rappel import workflow
        from rappel.workflow import Workflow

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
        from rappel import action, workflow
        from rappel.workflow import Workflow

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

        # Find conditional
        func = program.functions[0]
        conditional = None
        for stmt in func.body.statements:
            if stmt.HasField("conditional"):
                conditional = stmt.conditional
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
        from rappel import workflow
        from rappel.workflow import Workflow

        @workflow
        class ModuloWorkflow(Workflow):
            async def run(self, x: int, y: int) -> int:
                return x % y

        program = ModuloWorkflow.workflow_ir()
        assert program is not None

    def test_power_operator(self) -> None:
        """Test: x ** y power operator."""
        from rappel import workflow
        from rappel.workflow import Workflow

        @workflow
        class PowerWorkflow(Workflow):
            async def run(self, x: int, y: int) -> int:
                return x**y

        program = PowerWorkflow.workflow_ir()
        assert program is not None

    def test_floor_division(self) -> None:
        """Test: x // y floor division."""
        from rappel import workflow
        from rappel.workflow import Workflow

        @workflow
        class FloorDivWorkflow(Workflow):
            async def run(self, x: int, y: int) -> int:
                return x // y

        program = FloorDivWorkflow.workflow_ir()
        assert program is not None

    def test_true_division(self) -> None:
        """Test: x / y true division."""
        from rappel import workflow
        from rappel.workflow import Workflow

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
        from rappel import workflow
        from rappel.workflow import Workflow

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
        from rappel import workflow
        from rappel.workflow import Workflow

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
        from rappel import workflow
        from rappel.workflow import Workflow

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
        from rappel import workflow
        from rappel.workflow import Workflow

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

        # Find try/except
        func = program.functions[0]
        try_except = None
        for stmt in func.body.statements:
            if stmt.HasField("try_except"):
                try_except = stmt.try_except
                break

        assert try_except is not None, "Should have try/except"
        # The try_body should have a 'call' field with the action call extracted
        # and a 'targets' field with the assignment variable(s)
        assert try_except.try_body.HasField("call"), "Try body should have call extracted"
        assert try_except.try_body.call.HasField("action"), "Call should be an action"
        assert try_except.try_body.call.action.action_name == "try_action"
        assert list(try_except.try_body.targets) == ["result"], "Targets should be ['result']"


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
        assert policy.retry.max_retries == 3
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
        assert policy.retry.max_retries == 2
        assert "ValueError" in policy.retry.exception_types
        assert "KeyError" in policy.retry.exception_types

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


class TestSpreadAction:
    """Test spread action detection - converts to SpreadExpr in IR."""

    def test_spread_pattern_converts_to_spread_expr(self) -> None:
        """Test: asyncio.gather(*[action(item) for item in items]) -> SpreadExpr."""
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


class TestForLoopWithMultipleCalls:
    """Test for loop body wrapping with multiple calls."""

    def test_for_body_wrapped_to_function(self) -> None:
        """Test: for loop with multiple calls gets wrapped into synthetic function."""
        from tests.fixtures_for_loop.for_multiple_calls import ForMultipleCallsWorkflow

        program = ForMultipleCallsWorkflow.workflow_ir()

        # Should have implicit function generated
        assert len(program.functions) > 1, "Should have implicit function for wrapped body"

        # Find the for loop
        for_loop = None
        for fn in program.functions:
            if fn.name == "run":
                for stmt in fn.body.statements:
                    if stmt.HasField("for_loop"):
                        for_loop = stmt.for_loop
                        break

        assert for_loop is not None, "Should have for loop"

        # The body should be a function call to the wrapper
        body = for_loop.body
        assert body.HasField("call"), "Body should have call to wrapper function"


class TestConditionalWithMultipleCalls:
    """Test conditional body wrapping with multiple calls."""

    def test_if_body_wrapped_to_function(self) -> None:
        """Test: if branch with multiple calls gets wrapped into synthetic function."""
        from tests.fixtures_conditional.if_multiple_calls import IfMultipleCallsWorkflow

        program = IfMultipleCallsWorkflow.workflow_ir()

        # Should have implicit function generated
        assert len(program.functions) > 1, "Should have implicit function for wrapped body"


class TestTryExceptWithMultipleCalls:
    """Test try/except body wrapping with multiple calls."""

    def test_try_body_wrapped_to_function(self) -> None:
        """Test: try body with multiple calls gets wrapped into synthetic function."""
        from tests.fixtures_try_except.try_multiple_calls import TryMultipleCallsWorkflow

        program = TryMultipleCallsWorkflow.workflow_ir()

        # Should have implicit function generated
        assert len(program.functions) > 1, "Should have implicit function for wrapped body"


class TestUnsupportedPatternValidation:
    """Test that unsupported patterns raise UnsupportedPatternError with helpful messages."""

    def test_constructor_return_raises_error(self) -> None:
        """Test: return MyModel(...) raises UnsupportedPatternError."""
        from typing import cast

        import pytest

        from rappel.ir_builder import UnsupportedPatternError

        with pytest.raises(UnsupportedPatternError) as exc_info:
            from tests.fixtures_unsupported.constructor_return import ConstructorReturnWorkflow

            ConstructorReturnWorkflow.workflow_ir()

        error = cast(UnsupportedPatternError, exc_info.value)
        assert "MyResult" in error.message
        assert (
            "constructor" in error.message.lower() or "constructor" in error.recommendation.lower()
        )
        assert "@action" in error.recommendation

    def test_constructor_assignment_raises_error(self) -> None:
        """Test: x = MyClass(...) raises UnsupportedPatternError."""
        from typing import cast

        import pytest

        from rappel.ir_builder import UnsupportedPatternError

        with pytest.raises(UnsupportedPatternError) as exc_info:
            from tests.fixtures_unsupported.constructor_assignment import (
                ConstructorAssignmentWorkflow,
            )

            ConstructorAssignmentWorkflow.workflow_ir()

        error = cast(UnsupportedPatternError, exc_info.value)
        assert "Config" in error.message
        assert "@action" in error.recommendation

    def test_non_action_await_raises_error(self) -> None:
        """Test: await non_action_func() raises UnsupportedPatternError."""
        from typing import cast

        import pytest

        from rappel.ir_builder import UnsupportedPatternError

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

        from rappel.ir_builder import UnsupportedPatternError

        with pytest.raises(UnsupportedPatternError) as exc_info:
            from tests.fixtures_unsupported.fstring_usage import FstringWorkflow

            FstringWorkflow.workflow_ir()

        error = cast(UnsupportedPatternError, exc_info.value)
        assert "f-string" in error.message.lower() or "F-string" in error.message
        assert "@action" in error.recommendation

    def test_while_loop_raises_error(self) -> None:
        """Test: while loops raise UnsupportedPatternError."""
        from typing import cast

        import pytest

        from rappel.ir_builder import UnsupportedPatternError

        with pytest.raises(UnsupportedPatternError) as exc_info:
            from tests.fixtures_unsupported.while_loop import WhileLoopWorkflow

            WhileLoopWorkflow.workflow_ir()

        error = cast(UnsupportedPatternError, exc_info.value)
        assert "while" in error.message.lower()

    def test_list_comprehension_raises_error(self) -> None:
        """Test: list comprehensions outside gather raise UnsupportedPatternError."""
        from typing import cast

        import pytest

        from rappel.ir_builder import UnsupportedPatternError

        with pytest.raises(UnsupportedPatternError) as exc_info:
            from tests.fixtures_unsupported.list_comprehension import ListComprehensionWorkflow

            ListComprehensionWorkflow.workflow_ir()

        error = cast(UnsupportedPatternError, exc_info.value)
        assert "comprehension" in error.message.lower()

    def test_lambda_raises_error(self) -> None:
        """Test: lambda expressions raise UnsupportedPatternError."""
        from typing import cast

        import pytest

        from rappel.ir_builder import UnsupportedPatternError

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

        from rappel.ir_builder import UnsupportedPatternError

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

        from rappel.ir_builder import UnsupportedPatternError

        with pytest.raises(UnsupportedPatternError) as exc_info:
            from tests.fixtures_unsupported.match_workflow import MatchWorkflow

            MatchWorkflow.workflow_ir()

        error = cast(UnsupportedPatternError, exc_info.value)
        assert "match" in error.message.lower()
        assert "if/elif/else" in error.recommendation.lower()


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
