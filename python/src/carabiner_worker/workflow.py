from __future__ import annotations

import inspect
from functools import wraps
from threading import RLock
from typing import Any, ClassVar, Dict, Optional, TypeVar

from .workflow_dag import WorkflowDag, build_workflow_dag

TWorkflow = TypeVar("TWorkflow", bound="Workflow")


class Workflow:
    """Base class for workflow definitions."""

    name: ClassVar[Optional[str]] = None
    _workflow_dag: ClassVar[Optional[WorkflowDag]] = None
    _dag_lock: ClassVar[RLock] = RLock()

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
        cls.workflow_dag()
        return await run_impl(self, *args, **kwargs)

    cls.run = run_public  # type: ignore[assignment]
    workflow_registry.register(cls.short_name(), cls)
    return cls
