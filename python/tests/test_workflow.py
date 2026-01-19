import asyncio
import importlib
import os
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

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

    async def fake_execute_workflow(_payload: bytes) -> bytes:
        payload = serialize_result_payload("done")
        return payload.SerializeToString()

    monkeypatch.setattr(bridge, "execute_workflow", fake_execute_workflow)

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


@action
async def uppercase_token(token: str) -> str:
    return token.upper()


@workflow_decorator
class TestActionWorkflow(Workflow):
    async def run(self) -> str:
        token = await fetch_identifier(identifier="alpha")
        transformed = await uppercase_token(token=token)
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


def test_workflow_result_coerces_to_pydantic_model(monkeypatch: pytest.MonkeyPatch) -> None:
    class ResultModel(BaseModel):
        user_id: UUID
        created_at: datetime
        status: str

    @action
    async def build_result() -> ResultModel:
        return ResultModel(
            user_id=uuid4(),
            created_at=datetime(2024, 1, 2, 3, 4, 5),
            status="ok",
        )

    @workflow_decorator
    class ModelWorkflow(Workflow):
        async def run(self) -> ResultModel:
            return await build_result()

    response_user_id = uuid4()
    response_created_at = datetime(2024, 1, 2, 3, 4, 5)

    async def fake_execute_workflow(_payload: bytes) -> bytes:
        response = {
            "user_id": str(response_user_id),
            "created_at": response_created_at.isoformat(),
            "status": "ok",
        }
        payload = serialize_result_payload(response)
        return payload.SerializeToString()

    monkeypatch.setattr(bridge, "execute_workflow", fake_execute_workflow)

    result = asyncio.run(ModelWorkflow().run())

    assert isinstance(result, ResultModel)
    assert result.user_id == response_user_id
    assert result.created_at == response_created_at
    assert result.status == "ok"


def test_workflow_result_coerces_to_dataclass(monkeypatch: pytest.MonkeyPatch) -> None:
    @dataclass
    class ResultData:
        name: str
        count: int

    @action
    async def build_data() -> ResultData:
        return ResultData(name="demo", count=3)

    @workflow_decorator
    class DataWorkflow(Workflow):
        async def run(self) -> ResultData:
            return await build_data()

    async def fake_execute_workflow(_payload: bytes) -> bytes:
        response = {"name": "demo", "count": 3}
        payload = serialize_result_payload(response)
        return payload.SerializeToString()

    monkeypatch.setattr(bridge, "execute_workflow", fake_execute_workflow)

    result = asyncio.run(DataWorkflow().run())
    assert isinstance(result, ResultData)
    assert result.name == "demo"
    assert result.count == 3


def test_workflow_result_optional_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    @dataclass
    class OptionalData:
        name: str
        count: int

    @action
    async def build_optional() -> OptionalData:
        return OptionalData(name="demo", count=1)

    @workflow_decorator
    class OptionalWorkflow(Workflow):
        async def run(self) -> Optional[OptionalData]:
            return await build_optional()

    async def fake_execute_workflow(_payload: bytes) -> bytes:
        payload = serialize_result_payload(None)
        return payload.SerializeToString()

    monkeypatch.setattr(bridge, "execute_workflow", fake_execute_workflow)

    result = asyncio.run(OptionalWorkflow().run())
    assert result is None


def test_workflow_result_union_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    @dataclass
    class PrimaryData:
        name: str
        count: int

    class SecondaryModel(BaseModel):
        user_id: UUID
        status: str

    @action
    async def build_union() -> PrimaryData:
        return PrimaryData(name="demo", count=1)

    @workflow_decorator
    class UnionWorkflow(Workflow):
        async def run(self) -> PrimaryData | SecondaryModel:
            return await build_union()

    response_user_id = uuid4()

    async def fake_execute_workflow(_payload: bytes) -> bytes:
        response = {"user_id": str(response_user_id), "status": "ok"}
        payload = serialize_result_payload(response)
        return payload.SerializeToString()

    monkeypatch.setattr(bridge, "execute_workflow", fake_execute_workflow)

    result = asyncio.run(UnionWorkflow().run())
    assert isinstance(result, SecondaryModel)
    assert result.user_id == response_user_id
    assert result.status == "ok"


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


def test_workflow_blocking_false_returns_instance_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that _blocking=False returns immediately with instance_id without waiting."""
    os.environ.pop("PYTEST_CURRENT_TEST", None)

    run_instance_calls: list[bytes] = []
    wait_for_instance_calls: list[str] = []
    expected_instance_id = "00000000-0000-0000-0000-000000000999"

    async def fake_run_instance(payload: bytes) -> bridge.RunInstanceResult:
        run_instance_calls.append(payload)
        return bridge.RunInstanceResult(
            workflow_version_id="00000000-0000-0000-0000-000000000888",
            workflow_instance_id=expected_instance_id,
        )

    async def fake_wait_for_instance(*, instance_id: str, poll_interval_secs: float = 1.0) -> bytes:
        wait_for_instance_calls.append(instance_id)
        payload = serialize_result_payload("should_not_reach")
        return payload.SerializeToString()

    monkeypatch.setattr(bridge, "run_instance", fake_run_instance)
    monkeypatch.setattr(bridge, "wait_for_instance", fake_wait_for_instance)

    @workflow_decorator
    class NonBlockingWorkflow(Workflow):
        async def run(self, _blocking: bool = True) -> str:
            return "test"

    instance = NonBlockingWorkflow()

    # With _blocking=False, should return instance_id immediately
    result = asyncio.run(instance.run(_blocking=False))

    assert result == expected_instance_id
    assert len(run_instance_calls) == 1
    assert len(wait_for_instance_calls) == 0  # Should NOT have called wait_for_instance

    os.environ["PYTEST_CURRENT_TEST"] = "true"


def test_workflow_blocking_true_waits_for_result(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that _blocking=True (default) waits for the workflow result."""
    os.environ.pop("PYTEST_CURRENT_TEST", None)

    run_instance_calls: list[bytes] = []
    wait_for_instance_calls: list[str] = []
    expected_instance_id = "00000000-0000-0000-0000-000000000777"
    expected_result = "workflow_completed"

    async def fake_run_instance(payload: bytes) -> bridge.RunInstanceResult:
        run_instance_calls.append(payload)
        return bridge.RunInstanceResult(
            workflow_version_id="00000000-0000-0000-0000-000000000666",
            workflow_instance_id=expected_instance_id,
        )

    async def fake_wait_for_instance(*, instance_id: str, poll_interval_secs: float = 1.0) -> bytes:
        wait_for_instance_calls.append(instance_id)
        payload = serialize_result_payload(expected_result)
        return payload.SerializeToString()

    monkeypatch.setattr(bridge, "run_instance", fake_run_instance)
    monkeypatch.setattr(bridge, "wait_for_instance", fake_wait_for_instance)

    @workflow_decorator
    class BlockingWorkflow(Workflow):
        async def run(self, _blocking: bool = True) -> str:
            return "test"

    instance = BlockingWorkflow()

    # With _blocking=True (default), should wait and return result
    result = asyncio.run(instance.run(_blocking=True))

    assert result == expected_result
    assert len(run_instance_calls) == 1
    assert len(wait_for_instance_calls) == 1
    assert wait_for_instance_calls[0] == expected_instance_id

    os.environ["PYTEST_CURRENT_TEST"] = "true"
