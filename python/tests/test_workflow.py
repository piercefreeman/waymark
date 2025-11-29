import asyncio
import importlib
import io
import os
import sys
from collections.abc import Iterator

import pytest

from rappel import bridge
from rappel.actions import action, serialize_result_payload
from rappel.workflow_dag import WorkflowDag

workflow_module = importlib.import_module("rappel.workflow")
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


def test_workflow_result_uses_dag_return_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that workflow results are extracted using the DAG's return_variable.

    This is a regression test for a bug where the code hardcoded "result" as the
    key to look up in WorkflowNodeResult.variables, but the DAG actually uses
    the variable name from the return statement (e.g., "transformed" for `return transformed`)
    or __workflow_return for expression returns (e.g., `return ChainResult(...)`).

    The workflow decorator must use dag.return_variable when extracting the result.
    """
    from pydantic import BaseModel

    from rappel.workflow_runtime import WorkflowNodeResult

    os.environ.pop("PYTEST_CURRENT_TEST", None)

    class TestResult(BaseModel):
        value: str

    expected_result = TestResult(value="test_value")

    # Get the DAG's return variable - this is what the fix should use
    dag = VisualizationWorkflow.workflow_dag()
    return_var = dag.return_variable
    assert return_var is not None, "DAG should have a return_variable"

    async def fake_run_instance(payload: bytes) -> bridge.RunInstanceResult:
        return bridge.RunInstanceResult(
            workflow_version_id="00000000-0000-0000-0000-000000000100",
            workflow_instance_id="00000000-0000-0000-0000-000000000200",
        )

    async def fake_wait_for_instance(*, instance_id: str, poll_interval_secs: float = 1.0) -> bytes:
        # Return a WorkflowNodeResult with the key being dag.return_variable
        # Before the fix, the code looked for "result" key which would fail
        node_result = WorkflowNodeResult(variables={return_var: expected_result})
        payload = serialize_result_payload(node_result)
        return payload.SerializeToString()

    monkeypatch.setattr(bridge, "run_instance", fake_run_instance)
    monkeypatch.setattr(bridge, "wait_for_instance", fake_wait_for_instance)

    instance = VisualizationWorkflow()
    result = asyncio.run(instance.run())

    # The result should be the TestResult (or its dict representation), not None
    # Before the fix, this would return None because it looked for "result" key
    # instead of using dag.return_variable (which is "transformed" in this case)
    assert result is not None, (
        "Result should not be None - workflow results must use dag.return_variable "
        f"(which is {return_var!r}), not a hardcoded 'result' key"
    )
    # The result may come back as a dict (after serialization round-trip)
    if isinstance(result, dict):
        assert result["value"] == "test_value"
    else:
        assert isinstance(result, TestResult)
        assert result.value == "test_value"

    os.environ["PYTEST_CURRENT_TEST"] = "true"
