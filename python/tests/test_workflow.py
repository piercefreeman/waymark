from __future__ import annotations

import asyncio
import importlib
import os
from collections.abc import Iterator

import pytest

from carabiner_worker import bridge
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

    async def fake_run_instance(database_url: str, payload: bytes) -> int:
        calls.append(payload)
        assert database_url.endswith("mountaineer_daemons")
        return 123

    monkeypatch.setattr(bridge, "run_instance", fake_run_instance)

    @workflow_decorator
    class ProductionWorkflow(Workflow):
        async def run(self) -> str:
            return "should not execute"

    instance = ProductionWorkflow()
    version = asyncio.run(instance.run())
    assert version == 123
    assert len(calls) == 1
    # Subsequent runs reuse cached version id and do not re-register.
    version_again = asyncio.run(instance.run())
    assert version_again == 123
    assert len(calls) == 1
    os.environ["PYTEST_CURRENT_TEST"] = "true"
