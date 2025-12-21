import asyncio
import importlib
import os
from collections.abc import Iterator

import pytest

from proto import ast_pb2 as ir
from proto import messages_pb2 as pb2
from rappel import bridge
from rappel.actions import action, serialize_result_payload
from rappel.serialization import arguments_to_kwargs

workflow_module = importlib.import_module("rappel.workflow")
Workflow = workflow_module.Workflow
workflow_decorator = workflow_module.workflow
workflow_registry = workflow_module.workflow_registry


@pytest.fixture(autouse=True)
def reset_workflow_registry() -> Iterator[None]:
    workflow_registry.reset()
    yield
    workflow_registry.reset()


def test_workflow_decorator_registers_and_caches_ir(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that the workflow decorator builds and caches the IR."""
    program = ir.Program()
    calls: list[type[Workflow]] = []

    def fake_build(cls: type[Workflow]) -> ir.Program:
        calls.append(cls)
        return program

    monkeypatch.setattr(workflow_module, "build_workflow_ir", fake_build)

    @workflow_decorator
    class DemoWorkflow(Workflow):
        async def run(self) -> str:
            return "done"

    instance = DemoWorkflow()
    result = asyncio.run(instance.run())
    assert result == "done"
    assert calls == [DemoWorkflow]
    # running a second time should reuse the cached IR
    asyncio.run(instance.run())
    assert calls == [DemoWorkflow]
    assert workflow_registry.get("demoworkflow") is DemoWorkflow
    assert DemoWorkflow.workflow_ir() is program


def test_workflow_short_name_override() -> None:
    @workflow_decorator
    class CustomWorkflow(Workflow):
        name = "Inventory.Sync"

        async def run(self) -> str:
            return "ok"

    assert CustomWorkflow.short_name() == "Inventory.Sync"
    assert workflow_registry.get("Inventory.Sync") is CustomWorkflow
    assert asyncio.run(CustomWorkflow().run()) == "ok"


def test_workflow_registration_outside_pytest(monkeypatch: pytest.MonkeyPatch) -> None:
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    calls: list[bytes] = []
    wait_calls: list[str] = []
    wait_results = ["first", "second"]

    async def fake_run_instance(payload: bytes) -> bridge.RunInstanceResult:
        calls.append(payload)
        suffix = len(calls)
        return bridge.RunInstanceResult(
            workflow_version_id=f"00000000-0000-0000-0000-0000000001{suffix:02d}",
            workflow_instance_id=f"00000000-0000-0000-0000-0000000002{suffix:02d}",
        )

    async def fake_wait_for_instance(*, instance_id: str, poll_interval_secs: float = 1.0) -> bytes:
        wait_calls.append(instance_id)
        _ = poll_interval_secs  # unused in fake
        payload = serialize_result_payload(wait_results[len(wait_calls) - 1])
        return payload.SerializeToString()

    monkeypatch.setattr(bridge, "run_instance", fake_run_instance)
    monkeypatch.setattr(bridge, "wait_for_instance", fake_wait_for_instance)

    @workflow_decorator
    class ProductionWorkflow(Workflow):
        async def run(self) -> str:
            return "should not execute"

    instance = ProductionWorkflow()
    result = asyncio.run(instance.run())
    assert result == "first"
    assert len(calls) == 1
    assert wait_calls == ["00000000-0000-0000-0000-000000000201"]

    # Subsequent runs should invoke gRPC again and wait for their own instance id.
    result_again = asyncio.run(instance.run())
    assert result_again == "second"
    assert len(calls) == 2
    assert wait_calls[-1] == "00000000-0000-0000-0000-000000000202"
    os.environ["PYTEST_CURRENT_TEST"] = "true"


def test_workflow_registration_applies_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    captured: list[bytes] = []

    async def fake_run_instance(payload: bytes) -> bridge.RunInstanceResult:
        captured.append(payload)
        return bridge.RunInstanceResult(
            workflow_version_id="00000000-0000-0000-0000-000000000199",
            workflow_instance_id="00000000-0000-0000-0000-000000000299",
        )

    async def fake_wait_for_instance(*, instance_id: str, poll_interval_secs: float = 1.0) -> bytes:
        _ = instance_id
        _ = poll_interval_secs
        payload = serialize_result_payload("ok")
        return payload.SerializeToString()

    monkeypatch.setattr(bridge, "run_instance", fake_run_instance)
    monkeypatch.setattr(bridge, "wait_for_instance", fake_wait_for_instance)

    @workflow_decorator
    class DefaultArgWorkflow(Workflow):
        async def run(self, count: int = 5, name: str = "demo") -> str:
            return name

    instance = DefaultArgWorkflow()
    result = asyncio.run(instance.run())
    assert result == "ok"
    assert len(captured) == 1

    registration = pb2.WorkflowRegistration()
    registration.ParseFromString(captured[0])
    inputs = arguments_to_kwargs(registration.initial_context)
    assert inputs == {"count": 5, "name": "demo"}

    os.environ["PYTEST_CURRENT_TEST"] = "true"


def test_workflow_registration_overrides_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    captured: list[bytes] = []

    async def fake_run_instance(payload: bytes) -> bridge.RunInstanceResult:
        captured.append(payload)
        return bridge.RunInstanceResult(
            workflow_version_id="00000000-0000-0000-0000-000000000399",
            workflow_instance_id="00000000-0000-0000-0000-000000000499",
        )

    async def fake_wait_for_instance(*, instance_id: str, poll_interval_secs: float = 1.0) -> bytes:
        _ = instance_id
        _ = poll_interval_secs
        payload = serialize_result_payload("ok")
        return payload.SerializeToString()

    monkeypatch.setattr(bridge, "run_instance", fake_run_instance)
    monkeypatch.setattr(bridge, "wait_for_instance", fake_wait_for_instance)

    @workflow_decorator
    class DefaultOverrideWorkflow(Workflow):
        async def run(self, count: int = 5, name: str = "demo") -> str:
            return name

    instance = DefaultOverrideWorkflow()
    result = asyncio.run(instance.run(count=9))
    assert result == "ok"
    assert len(captured) == 1

    registration = pb2.WorkflowRegistration()
    registration.ParseFromString(captured[0])
    inputs = arguments_to_kwargs(registration.initial_context)
    assert inputs == {"count": 9, "name": "demo"}

    os.environ["PYTEST_CURRENT_TEST"] = "true"


@action
async def fetch_identifier(identifier: str) -> str:
    raise NotImplementedError


@action
async def store_value(result: str) -> None:
    raise NotImplementedError


@workflow_decorator
class TestActionWorkflow(Workflow):
    async def run(self) -> str:
        token = await fetch_identifier(identifier="alpha")
        transformed = token.upper()
        await store_value(result=transformed)
        return transformed


def test_workflow_builds_ir() -> None:
    """Test that a workflow builds a valid IR Program."""
    program = TestActionWorkflow.workflow_ir()
    assert isinstance(program, ir.Program)
    assert len(program.functions) == 1
    func = program.functions[0]
    assert func.name == "main"


def test_workflow_ir_includes_module_name() -> None:
    """Test that the IR includes module_name for action calls."""
    program = TestActionWorkflow.workflow_ir()
    func = program.functions[0]

    # Find action calls in the IR (both direct statements and in assignments)
    action_calls_found = []
    for stmt in func.body.statements:
        if stmt.HasField("action_call"):
            action_calls_found.append(stmt.action_call)
        elif stmt.HasField("assignment"):
            if stmt.assignment.value.HasField("action_call"):
                action_calls_found.append(stmt.assignment.value.action_call)

    # We should have at least 2 action calls (fetch_identifier, store_value)
    assert len(action_calls_found) >= 2

    # Each action call should have a module_name set
    for action_call in action_calls_found:
        assert action_call.action_name, "action_name should be set"
        assert action_call.module_name, (
            f"module_name should be set for action {action_call.action_name}"
        )
        # The module should be this test module
        assert "test_workflow" in action_call.module_name


def test_workflow_result_direct_return(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that workflow results are returned directly when not using WorkflowNodeResult."""
    os.environ.pop("PYTEST_CURRENT_TEST", None)

    expected_result = "test_value"

    async def fake_run_instance(payload: bytes) -> bridge.RunInstanceResult:
        return bridge.RunInstanceResult(
            workflow_version_id="00000000-0000-0000-0000-000000000100",
            workflow_instance_id="00000000-0000-0000-0000-000000000200",
        )

    async def fake_wait_for_instance(*, instance_id: str, poll_interval_secs: float = 1.0) -> bytes:
        # Return a direct result (not wrapped in WorkflowNodeResult)
        payload = serialize_result_payload(expected_result)
        return payload.SerializeToString()

    monkeypatch.setattr(bridge, "run_instance", fake_run_instance)
    monkeypatch.setattr(bridge, "wait_for_instance", fake_wait_for_instance)

    @workflow_decorator
    class SimpleWorkflow(Workflow):
        async def run(self) -> str:
            return "test"

    instance = SimpleWorkflow()
    result = asyncio.run(instance.run())

    assert result == expected_result

    os.environ["PYTEST_CURRENT_TEST"] = "true"
