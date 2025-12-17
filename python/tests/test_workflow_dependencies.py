"""Tests for dependency injection in workflows running under pytest.

This module tests that Depend() markers are properly resolved when workflows
are run directly under pytest (bypassing the gRPC bridge).
"""

import asyncio
from typing import Annotated

import pytest

from rappel import Workflow, workflow
from rappel.actions import action
from rappel.dependencies import Depend

# Track whether dependencies were actually resolved
dependency_calls: list[str] = []


async def provide_database_connection() -> str:
    """Dependency that provides a database connection string."""
    dependency_calls.append("database")
    return "db://localhost:5432/test"


async def provide_cache_client() -> str:
    """Dependency that provides a cache client."""
    dependency_calls.append("cache")
    return "redis://localhost:6379"


@action
async def action_with_dependency(
    value: int,
    db_conn: Annotated[str, Depend(provide_database_connection)],
) -> str:
    """Action that requires a database connection dependency."""
    return f"processed {value} with {db_conn}"


@action
async def action_with_multiple_dependencies(
    value: int,
    db_conn: Annotated[str, Depend(provide_database_connection)],
    cache: Annotated[str, Depend(provide_cache_client)],
) -> str:
    """Action that requires multiple dependencies."""
    return f"processed {value} with {db_conn} and {cache}"


@workflow
class WorkflowWithDependentAction(Workflow):
    """Workflow that uses an action with dependencies."""

    async def run(self, value: int = 42) -> str:
        result = await action_with_dependency(value=value)  # type: ignore[call-arg]
        return result


@workflow
class WorkflowWithMultipleDependencies(Workflow):
    """Workflow that uses an action with multiple dependencies."""

    async def run(self, value: int = 100) -> str:
        result = await action_with_multiple_dependencies(value=value)  # type: ignore[call-arg]
        return result


@pytest.fixture(autouse=True)
def clear_dependency_calls():
    """Clear the dependency call tracker before each test."""
    dependency_calls.clear()
    yield
    dependency_calls.clear()


class TestWorkflowDependencyResolution:
    """Test that dependencies are resolved when workflows run under pytest."""

    def test_action_with_dependency_called_directly(self):
        """Test that calling an action directly resolves dependencies automatically.

        The @action decorator wraps functions with dependencies so that Depend()
        markers are resolved when the action is called directly (not just through
        execute_action).
        """
        # When called directly, the dependency SHOULD be resolved automatically
        # because the @action decorator wraps the function
        result = asyncio.run(action_with_dependency(value=10))  # type: ignore[call-arg]

        # Dependencies should be resolved and the action should execute successfully
        assert result == "processed 10 with db://localhost:5432/test"
        assert "database" in dependency_calls

    def test_workflow_with_dependency_runs_under_pytest(self):
        """Test that a workflow with dependent actions runs under pytest.

        This test will fail if dependencies are not being resolved properly
        when the workflow runs under pytest (PYTEST_CURRENT_TEST is set).
        """
        wf = WorkflowWithDependentAction()

        # This should work if dependencies are properly resolved
        # Under pytest, workflow.run() directly executes the original run_impl
        result = asyncio.run(wf.run(value=42))

        assert result == "processed 42 with db://localhost:5432/test"
        assert "database" in dependency_calls

    def test_workflow_with_multiple_dependencies(self):
        """Test workflow with multiple dependencies."""
        wf = WorkflowWithMultipleDependencies()

        result = asyncio.run(wf.run(value=100))

        assert result == "processed 100 with db://localhost:5432/test and redis://localhost:6379"
        assert "database" in dependency_calls
        assert "cache" in dependency_calls


class TestDependencyResolutionMechanism:
    """Test the mechanism for resolving dependencies in pytest context."""

    def test_dependency_not_resolved_without_fixture(self):
        """Verify that the annotation contains DependMarker.

        This test checks that the Depend() markers are properly stored
        in the type annotations.
        """
        # Get the wrapped function's signature to inspect the annotations
        import inspect

        from rappel.dependencies import DependMarker

        # The wrapper has __wrapped__ pointing to the original function
        original_func = action_with_dependency.__wrapped__  # type: ignore[attr-defined]
        sig = inspect.signature(original_func)
        db_conn_param = sig.parameters["db_conn"]

        # The annotation contains a DependMarker
        from typing import get_args, get_origin

        assert get_origin(db_conn_param.annotation) is Annotated
        args = get_args(db_conn_param.annotation)
        assert any(isinstance(arg, DependMarker) for arg in args)

    def test_workflow_run_impl_preserved(self):
        """Test that the original run implementation is preserved."""
        # The workflow decorator should preserve the original implementation
        assert hasattr(WorkflowWithDependentAction, "__workflow_run_impl__")
        original_run = WorkflowWithDependentAction.__workflow_run_impl__
        assert asyncio.iscoroutinefunction(original_run)


class TestEdgeCases:
    """Test edge cases for dependency injection in workflows."""

    def test_action_without_dependencies(self):
        """Test that actions without dependencies still work correctly."""

        @action
        async def simple_action(x: int, y: int) -> int:
            return x + y

        result = asyncio.run(simple_action(3, 4))
        assert result == 7

    def test_nested_dependencies_in_workflow(self):
        """Test workflow with nested dependencies (dependency has a dependency)."""
        call_order: list[str] = []

        async def base_dependency() -> str:
            call_order.append("base")
            return "base_value"

        async def nested_dependency(base: Annotated[str, Depend(base_dependency)]) -> str:
            call_order.append("nested")
            return f"nested_{base}"

        @action
        async def action_with_nested_deps(data: Annotated[str, Depend(nested_dependency)]) -> str:
            return f"result_{data}"

        @workflow
        class WorkflowWithNestedDeps(Workflow):
            async def run(self) -> str:
                return await action_with_nested_deps()  # type: ignore[call-arg]

        wf = WorkflowWithNestedDeps()
        result = asyncio.run(wf.run())

        assert result == "result_nested_base_value"
        assert call_order == ["base", "nested"]

    def test_async_generator_dependency_in_workflow(self):
        """Test workflow with async generator dependencies (resource cleanup)."""
        resource_state: list[str] = []

        async def async_resource():
            resource_state.append("acquired")
            yield "resource_handle"
            resource_state.append("released")

        @action
        async def action_with_async_generator(
            resource: Annotated[str, Depend(async_resource)],
        ) -> str:
            return f"used_{resource}"

        @workflow
        class WorkflowWithAsyncGenerator(Workflow):
            async def run(self) -> str:
                return await action_with_async_generator()  # type: ignore[call-arg]

        wf = WorkflowWithAsyncGenerator()
        result = asyncio.run(wf.run())

        assert result == "used_resource_handle"
        assert resource_state == ["acquired", "released"]

    def test_sync_generator_dependency_in_workflow(self):
        """Test workflow with sync generator dependencies."""
        resource_state: list[str] = []

        def sync_resource():
            resource_state.append("acquired")
            yield "sync_handle"
            resource_state.append("released")

        @action
        async def action_with_sync_generator(
            resource: Annotated[str, Depend(sync_resource)],
        ) -> str:
            return f"used_{resource}"

        @workflow
        class WorkflowWithSyncGenerator(Workflow):
            async def run(self) -> str:
                return await action_with_sync_generator()  # type: ignore[call-arg]

        wf = WorkflowWithSyncGenerator()
        result = asyncio.run(wf.run())

        assert result == "used_sync_handle"
        assert resource_state == ["acquired", "released"]

    def test_action_with_positional_args(self):
        """Test that positional args are correctly converted to kwargs."""

        @action
        async def action_with_positional(a: int, b: str, c: float = 1.0) -> str:
            return f"{a}-{b}-{c}"

        # Call with positional args
        result = asyncio.run(action_with_positional(42, "hello", 3.14))
        assert result == "42-hello-3.14"

        # Call with mixed args
        result = asyncio.run(action_with_positional(10, "world"))
        assert result == "10-world-1.0"

    def test_action_preserves_wrapped_reference(self):
        """Test that the wrapper preserves reference to original function."""

        @action
        async def original_action(x: int) -> int:
            return x * 2

        # Should have __wrapped__ attribute pointing to original
        assert hasattr(original_action, "__wrapped__")
        assert original_action.__wrapped__.__name__ == "original_action"

    def test_multiple_workflows_with_same_dependency(self):
        """Test that dependency caching works correctly across workflow calls."""
        call_count = [0]  # Use list to allow mutation in closure

        async def counted_dependency() -> str:
            call_count[0] += 1
            return f"call_{call_count[0]}"

        @action
        async def action_with_counted_dep(value: Annotated[str, Depend(counted_dependency)]) -> str:
            return value

        @workflow
        class WorkflowWithCountedDep(Workflow):
            async def run(self) -> str:
                return await action_with_counted_dep()  # type: ignore[call-arg]

        # Each workflow run should get a fresh dependency resolution
        wf = WorkflowWithCountedDep()
        result1 = asyncio.run(wf.run())
        result2 = asyncio.run(wf.run())

        assert result1 == "call_1"
        assert result2 == "call_2"
        assert call_count[0] == 2
