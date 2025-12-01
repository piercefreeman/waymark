"""
Comprehensive tests for IRParser error cases.

These tests verify that the IRParser produces helpful error messages
when encountering invalid workflow patterns.
"""

import ast
import textwrap

import pytest

from rappel.ir import ActionDefinition, IRParseError, IRParser

# =============================================================================
# Test Fixtures
# =============================================================================

EXAMPLE_MODULE = """
import asyncio

async def fetch_left() -> int: ...
async def fetch_right() -> int: ...
async def double(value: int) -> int: ...
async def risky_action() -> int: ...
async def fallback_action() -> int: ...


class Workflow:
    pass
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


def parse_workflow(code: str, action_defs: dict[str, ActionDefinition]):
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

    parser = IRParser(action_defs=action_defs)
    return parser.parse_workflow(run_method)


# =============================================================================
# Conditional Error Tests
# =============================================================================


class TestConditionalErrors:
    """Test error cases for conditional statements."""

    def test_missing_else_branch(self, action_defs: dict[str, ActionDefinition]):
        """Conditional with action but no else branch should error."""
        code = """
class MissingElseWorkflow(Workflow):
    async def run(self, x: int) -> int:
        if x > 0:
            result = await fetch_left()
        return result
"""
        with pytest.raises(IRParseError) as exc_info:
            parse_workflow(code, action_defs)

        assert "requires an else branch" in str(exc_info.value).lower()

    def test_empty_branch_no_action(self, action_defs: dict[str, ActionDefinition]):
        """Branch with no action should error."""
        code = """
class EmptyBranchWorkflow(Workflow):
    async def run(self, x: int) -> int:
        if x > 0:
            result = await fetch_left()
        else:
            result = 42  # No action in else branch
        return result
"""
        with pytest.raises(IRParseError) as exc_info:
            parse_workflow(code, action_defs)

        assert "must have at least one action" in str(exc_info.value).lower()

    def test_action_after_postamble(self, action_defs: dict[str, ActionDefinition]):
        """Action after non-action statement in branch should error."""
        code = """
class ActionAfterPostambleWorkflow(Workflow):
    async def run(self, x: int) -> int:
        if x > 0:
            a = await fetch_left()
            label = "positive"  # postamble
            b = await fetch_right()  # Action after postamble
        else:
            a = await fetch_right()
        return a
"""
        with pytest.raises(IRParseError) as exc_info:
            parse_workflow(code, action_defs)

        assert "cannot appear after non-action" in str(exc_info.value).lower()


# =============================================================================
# Loop Error Tests
# =============================================================================


class TestLoopErrors:
    """Test error cases for loop statements."""

    def test_for_else_not_supported(self, action_defs: dict[str, ActionDefinition]):
        """for/else should error."""
        code = """
class ForElseWorkflow(Workflow):
    async def run(self, items: list) -> list:
        results = []
        for item in items:
            r = await double(value=item)
            results.append(r)
        else:
            pass
        return results
"""
        with pytest.raises(IRParseError) as exc_info:
            parse_workflow(code, action_defs)

        assert "for/else is not supported" in str(exc_info.value).lower()

    def test_loop_no_accumulator(self, action_defs: dict[str, ActionDefinition]):
        """Loop without append to accumulator should error."""
        code = """
class NoAccumulatorWorkflow(Workflow):
    async def run(self, items: list) -> None:
        for item in items:
            await double(value=item)
"""
        with pytest.raises(IRParseError) as exc_info:
            parse_workflow(code, action_defs)

        assert "must append to an accumulator" in str(exc_info.value).lower()

    def test_loop_no_action(self, action_defs: dict[str, ActionDefinition]):
        """Loop without action should error."""
        code = """
class NoActionInLoopWorkflow(Workflow):
    async def run(self, items: list) -> list:
        results = []
        for item in items:
            results.append(item * 2)
        return results
"""
        with pytest.raises(IRParseError) as exc_info:
            parse_workflow(code, action_defs)

        assert "must contain at least one action" in str(exc_info.value).lower()

    def test_tuple_unpacking_not_supported(self, action_defs: dict[str, ActionDefinition]):
        """Tuple unpacking in for loop should error."""
        code = """
class TupleUnpackWorkflow(Workflow):
    async def run(self, items: list) -> list:
        results = []
        for a, b in items:
            r = await double(value=a)
            results.append(r)
        return results
"""
        with pytest.raises(IRParseError) as exc_info:
            parse_workflow(code, action_defs)

        assert "must be a simple variable" in str(exc_info.value).lower()

    def test_statement_after_first_action(self, action_defs: dict[str, ActionDefinition]):
        """Non-action statement after first action in loop should error."""
        code = """
class StatementAfterActionWorkflow(Workflow):
    async def run(self, items: list) -> list:
        results = []
        for item in items:
            first = await fetch_left()
            intermediate = first * 2  # Non-action after action
            second = await double(value=intermediate)
            results.append(second)
        return results
"""
        with pytest.raises(IRParseError) as exc_info:
            parse_workflow(code, action_defs)

        assert "non-action statements after first action" in str(exc_info.value).lower()


# =============================================================================
# Try/Except Error Tests
# =============================================================================


class TestTryExceptErrors:
    """Test error cases for try/except statements."""

    def test_finally_not_supported(self, action_defs: dict[str, ActionDefinition]):
        """finally block should error."""
        code = """
class TryFinallyWorkflow(Workflow):
    async def run(self) -> int:
        try:
            result = await risky_action()
        except ValueError:
            result = await fallback_action()
        finally:
            pass
        return result
"""
        with pytest.raises(IRParseError) as exc_info:
            parse_workflow(code, action_defs)

        assert "finally blocks are not supported" in str(exc_info.value).lower()

    def test_try_else_not_supported(self, action_defs: dict[str, ActionDefinition]):
        """try/else should error."""
        code = """
class TryElseWorkflow(Workflow):
    async def run(self) -> int:
        try:
            result = await risky_action()
        except ValueError:
            result = await fallback_action()
        else:
            result = await fetch_left()
        return result
"""
        with pytest.raises(IRParseError) as exc_info:
            parse_workflow(code, action_defs)

        assert "try/else is not supported" in str(exc_info.value).lower()

    def test_exception_binding_not_supported(self, action_defs: dict[str, ActionDefinition]):
        """Exception variable binding should error."""
        code = """
class ExceptionBindingWorkflow(Workflow):
    async def run(self) -> int:
        try:
            result = await risky_action()
        except ValueError as e:
            result = await fallback_action()
        return result
"""
        with pytest.raises(IRParseError) as exc_info:
            parse_workflow(code, action_defs)

        assert "cannot bind exception to variable" in str(exc_info.value).lower()

    def test_try_body_must_be_actions(self, action_defs: dict[str, ActionDefinition]):
        """Non-action in try body should error."""
        code = """
class TryNonActionWorkflow(Workflow):
    async def run(self) -> int:
        try:
            x = 10  # Non-action
            result = await risky_action()
        except ValueError:
            result = await fallback_action()
        return result
"""
        with pytest.raises(IRParseError) as exc_info:
            parse_workflow(code, action_defs)

        assert "try block must contain only action calls" in str(exc_info.value).lower()

    def test_except_body_must_be_actions(self, action_defs: dict[str, ActionDefinition]):
        """Non-action in except body should error."""
        code = """
class ExceptNonActionWorkflow(Workflow):
    async def run(self) -> int:
        try:
            result = await risky_action()
        except ValueError:
            x = 10  # Non-action
            result = x
        return result
"""
        with pytest.raises(IRParseError) as exc_info:
            parse_workflow(code, action_defs)

        assert "except block must contain only action calls" in str(exc_info.value).lower()


# =============================================================================
# run_action Error Tests
# =============================================================================


class TestRunActionErrors:
    """Test error cases for run_action calls."""

    def test_unknown_action_in_run_action(self, action_defs: dict[str, ActionDefinition]):
        """Unknown action in run_action should error."""
        code = """
class UnknownActionWorkflow(Workflow):
    async def run(self) -> int:
        result = await self.run_action(unknown_action())
        return result
"""
        with pytest.raises(IRParseError) as exc_info:
            parse_workflow(code, action_defs)

        assert "unknown action" in str(exc_info.value).lower()

    def test_run_action_no_argument(self, action_defs: dict[str, ActionDefinition]):
        """run_action without argument should error."""
        code = """
class NoArgRunActionWorkflow(Workflow):
    async def run(self) -> int:
        result = await self.run_action()
        return result
"""
        with pytest.raises(IRParseError) as exc_info:
            parse_workflow(code, action_defs)

        assert "requires an action argument" in str(exc_info.value).lower()


# =============================================================================
# Gather Error Tests
# =============================================================================


class TestGatherErrors:
    """Test error cases for gather statements."""

    def test_gather_non_action_argument(self, action_defs: dict[str, ActionDefinition]):
        """Non-action argument to gather should error."""
        code = """
class GatherNonActionWorkflow(Workflow):
    async def run(self) -> tuple:
        results = await asyncio.gather(fetch_left(), some_non_action())
        return results
"""
        with pytest.raises(IRParseError) as exc_info:
            parse_workflow(code, action_defs)

        assert "gather argument must be an action call" in str(exc_info.value).lower()


# =============================================================================
# Sleep Error Tests
# =============================================================================


class TestSleepErrors:
    """Test error cases for sleep statements."""

    def test_sleep_no_duration(self, action_defs: dict[str, ActionDefinition]):
        """Sleep without duration should error."""
        code = """
class SleepNoDurationWorkflow(Workflow):
    async def run(self) -> int:
        await asyncio.sleep()
        result = await fetch_left()
        return result
"""
        with pytest.raises(IRParseError) as exc_info:
            parse_workflow(code, action_defs)

        assert "requires a duration argument" in str(exc_info.value).lower()


# =============================================================================
# Source Location Tests
# =============================================================================


class TestSourceLocations:
    """Test that errors include useful source locations."""

    def test_error_includes_line_number(self, action_defs: dict[str, ActionDefinition]):
        """Errors should include line numbers."""
        code = """
class LocationTestWorkflow(Workflow):
    async def run(self, x: int) -> int:
        if x > 0:
            result = await fetch_left()
        return result
"""
        with pytest.raises(IRParseError) as exc_info:
            parse_workflow(code, action_defs)

        error_msg = str(exc_info.value)
        # Should include some location reference
        assert "line" in error_msg.lower() or "(line" in error_msg.lower()


# =============================================================================
# Guard Validation Tests
# =============================================================================


class TestGuardValidation:
    """Test guard expression validation errors."""

    def test_lambda_in_guard_not_allowed(self, action_defs: dict[str, ActionDefinition]):
        """Lambda in guard expression should error."""
        code = """
class LambdaGuardWorkflow(Workflow):
    async def run(self, items: list) -> str:
        if (lambda x: x > 0)(10):
            result = await fetch_left()
        else:
            result = await fetch_right()
        return result
"""
        with pytest.raises(IRParseError) as exc_info:
            parse_workflow(code, action_defs)

        assert "lambda" in str(exc_info.value).lower()

    def test_forbidden_function_in_guard(self, action_defs: dict[str, ActionDefinition]):
        """Forbidden function in guard should error."""
        code = """
class ForbiddenFunctionGuardWorkflow(Workflow):
    async def run(self, items: list) -> str:
        if open("/etc/passwd"):
            result = await fetch_left()
        else:
            result = await fetch_right()
        return result
"""
        with pytest.raises(IRParseError) as exc_info:
            parse_workflow(code, action_defs)

        assert "not allowed in guard" in str(exc_info.value).lower()


# =============================================================================
# Edge Case Tests
# =============================================================================


class TestEdgeCases:
    """Test edge cases and corner cases."""

    def test_empty_try_body(self, action_defs: dict[str, ActionDefinition]):
        """Empty try body should error."""
        code = """
class EmptyTryWorkflow(Workflow):
    async def run(self) -> int:
        try:
            pass
        except ValueError:
            result = await fallback_action()
        return result
"""
        with pytest.raises(IRParseError) as exc_info:
            parse_workflow(code, action_defs)

        # Should error about needing at least one action
        error_msg = str(exc_info.value).lower()
        assert "action" in error_msg

    def test_deeply_nested_conditional_error(self, action_defs: dict[str, ActionDefinition]):
        """Deeply nested conditional without else should error."""
        code = """
class DeepNestedWorkflow(Workflow):
    async def run(self, x: int) -> int:
        if x > 100:
            if x > 200:
                result = await fetch_left()
            else:
                result = await fetch_right()
        return result
"""
        # The outer if doesn't have an else
        with pytest.raises(IRParseError) as exc_info:
            parse_workflow(code, action_defs)

        assert "else branch" in str(exc_info.value).lower()
