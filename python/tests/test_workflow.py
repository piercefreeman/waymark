from __future__ import annotations

import asyncio
import importlib
import io
import os
import sys
from collections.abc import Iterator

import pytest

from carabiner_worker import bridge
from carabiner_worker.actions import action, serialize_result_payload
from carabiner_worker.workflow_dag import WorkflowDag

workflow_module = importlib.import_module("carabiner_worker.workflow")
Workflow = workflow_module.Workflow
workflow_decorator = workflow_module.workflow
workflow_registry = workflow_module.workflow_registry


@pytest.fixture(autouse=True)
def reset_workflow_registry() -> Iterator[None]:
    workflow_registry.reset()
    yield
    workflow_registry.reset()


def test_workflow_decorator_registers_and_caches_dag(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = WorkflowDag(nodes=[])
    calls: list[type[Workflow]] = []

    def fake_build(cls: type[Workflow]) -> WorkflowDag:
        calls.append(cls)
        return dag

    monkeypatch.setattr(workflow_module, "build_workflow_dag", fake_build)

    @workflow_decorator
    class DemoWorkflow(Workflow):
        async def run(self) -> str:
            return "done"

    instance = DemoWorkflow()
    result = asyncio.run(instance.run())
    assert result == "done"
    assert calls == [DemoWorkflow]
    # running a second time should reuse the cached DAG
    asyncio.run(instance.run())
    assert calls == [DemoWorkflow]
    assert workflow_registry.get("demoworkflow") is DemoWorkflow
    assert DemoWorkflow.workflow_dag() is dag


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


@action
async def viz_fetch_identifier(identifier: str) -> str:
    raise NotImplementedError


@action
async def viz_store_value(result: str) -> None:
    raise NotImplementedError


@workflow_decorator
class VisualizationWorkflow(Workflow):
    async def run(self) -> str:
        token = await viz_fetch_identifier(identifier="alpha")
        transformed = token.upper()
        await viz_store_value(result=transformed)
        return transformed


def test_workflow_visualize_outputs_ascii_summary() -> None:
    buffer = io.StringIO()
    output = VisualizationWorkflow.visualize(stream=buffer)
    assert buffer.getvalue().rstrip("\n") == output

    # print to stdout so we can visualize it - we write it to stdout directly because
    # stringio has tty=False so it doesn't support colors
    VisualizationWorkflow.visualize(stream=sys.stdout)
    sys.stdout.flush()

    module_line = f"Workflow: {VisualizationWorkflow.__module__}.{VisualizationWorkflow.__name__}"
    assert module_line in output
    dag = VisualizationWorkflow.workflow_dag()
    expected_return_line = f"{'Return var':<12}: {dag.return_variable}"
    assert expected_return_line in output
    assert "Graph:" in output
    module_name = VisualizationWorkflow.__module__
    assert f"| [{module_name}]" in output
    assert "| node_0" in output
    assert "| python_block" in output
    assert "└──▶" in output
    assert "Details:" in output
    assert "node_0: viz_fetch_identifier" in output
    assert "node_2: viz_store_value" in output
    assert "      - identifier: 'alpha'" in output
    assert "      - result: transformed" in output


class _TtyBuffer(io.StringIO):
    def isatty(self) -> bool:  # type: ignore[override]
        return True


def test_workflow_visualize_placeholders_dim() -> None:
    buffer = _TtyBuffer()
    VisualizationWorkflow.visualize(stream=buffer)
    output = buffer.getvalue()
    assert "\u001b[2m    guard       : -\u001b[0m" in output
