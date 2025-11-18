from __future__ import annotations

import hashlib
import inspect
import os
from functools import wraps
from threading import RLock
from typing import Any, ClassVar, Dict, Optional, TypeVar

from proto import messages_pb2 as pb2

from . import bridge
from .workflow_dag import WorkflowDag, build_workflow_dag

TWorkflow = TypeVar("TWorkflow", bound="Workflow")


class Workflow:
    """Base class for workflow definitions."""

    name: ClassVar[Optional[str]] = None
    """Human-friendly identifier. Override to pin the registry key; defaults to lowercase class name."""

    concurrent: ClassVar[bool] = False
    """When True, downstream engines may respect DAG-parallel execution; False preserves sequential semantics which
    is what you would see if you were to run the code directly in a Python process."""

    database_url: ClassVar[Optional[str]] = None
    """Override to point to a non-default DATABASE_URL when registering workflow definitions."""

    _workflow_dag: ClassVar[Optional[WorkflowDag]] = None
    _dag_lock: ClassVar[RLock] = RLock()
    _workflow_version_id: ClassVar[Optional[int]] = None

    async def run(self) -> Any:
        raise NotImplementedError

    @classmethod
    def short_name(cls) -> str:
        if cls.name:
            return cls.name
        return cls.__name__.lower()

    @classmethod
    def workflow_dag(cls) -> WorkflowDag:
        if cls._workflow_dag is None:
            with cls._dag_lock:
                if cls._workflow_dag is None:
                    cls._workflow_dag = build_workflow_dag(cls)
        return cls._workflow_dag

    @classmethod
    def database_dsn(cls) -> str:
        if cls.database_url:
            return cls.database_url
        return os.environ.get(
            "DATABASE_URL",
            "postgres://mountaineer:mountaineer@localhost:5433/mountaineer_daemons",
        )

    @classmethod
    def _build_registration_payload(cls) -> pb2.WorkflowRegistration:
        dag = cls.workflow_dag()
        dag_definition = pb2.WorkflowDagDefinition(concurrent=cls.concurrent)
        for node in dag.nodes:
            proto_node = pb2.WorkflowDagNode(
                id=node.id,
                action=node.action,
                module=node.module or "",
                depends_on=list(node.depends_on),
                wait_for_sync=list(node.wait_for_sync),
                produces=list(node.produces),
            )
            proto_node.kwargs.update(node.kwargs)
            if node.guard:
                proto_node.guard = node.guard
            dag_definition.nodes.append(proto_node)
        dag_bytes = dag_definition.SerializeToString()
        dag_hash = hashlib.sha256(dag_bytes).hexdigest()
        message = pb2.WorkflowRegistration(
            workflow_name=cls.short_name(),
            dag=dag_definition,
            dag_hash=dag_hash,
        )
        return message


class WorkflowRegistry:
    """Registry of workflow definitions keyed by workflow name."""

    def __init__(self) -> None:
        self._workflows: Dict[str, type[Workflow]] = {}
        self._lock = RLock()

    def register(self, name: str, workflow_cls: type[Workflow]) -> None:
        with self._lock:
            if name in self._workflows:
                raise ValueError(f"workflow '{name}' already registered")
            self._workflows[name] = workflow_cls

    def get(self, name: str) -> Optional[type[Workflow]]:
        with self._lock:
            return self._workflows.get(name)

    def names(self) -> list[str]:
        with self._lock:
            return sorted(self._workflows.keys())

    def reset(self) -> None:
        with self._lock:
            self._workflows.clear()


workflow_registry = WorkflowRegistry()


def workflow(cls: type[TWorkflow]) -> type[TWorkflow]:
    """Decorator that registers workflow classes and caches their DAGs."""

    if not issubclass(cls, Workflow):
        raise TypeError("workflow decorator requires Workflow subclasses")
    run_impl = cls.run
    if not inspect.iscoroutinefunction(run_impl):
        raise TypeError("workflow run() must be defined with 'async def'")

    @wraps(run_impl)
    async def run_public(self: Workflow, *args: Any, **kwargs: Any) -> Any:
        if _running_under_pytest():
            cls.workflow_dag()
            return await run_impl(self, *args, **kwargs)
        version = cls._workflow_version_id
        if version is None:
            payload = cls._build_registration_payload()
            version = await bridge.run_instance(cls.database_dsn(), payload.SerializeToString())
            cls._workflow_version_id = version
        return version

    cls.__workflow_run_impl__ = run_impl
    cls.run = run_public  # type: ignore[assignment]
    workflow_registry.register(cls.short_name(), cls)
    return cls


def _running_under_pytest() -> bool:
    return bool(os.environ.get("PYTEST_CURRENT_TEST"))
