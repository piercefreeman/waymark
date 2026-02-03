"""Backend interfaces for persisting runner state and action results."""

from __future__ import annotations

import contextlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence, TYPE_CHECKING
from uuid import UUID, uuid4

if TYPE_CHECKING:  # pragma: no cover - type checkers only
    from ..dag import DAG
    from ..runner.state import ExecutionEdge, ExecutionNode


@dataclass(frozen=True)
class QueuedInstance:
    """Queued instance payload for the run loop."""

    dag: "DAG"
    entry_node: UUID
    state: "RunnerState | None" = None
    nodes: "Mapping[UUID, ExecutionNode] | None" = None
    edges: "Iterable[ExecutionEdge] | None" = None
    action_results: "Mapping[UUID, Any] | None" = None
    instance_id: UUID = field(default_factory=uuid4)

    def __setstate__(self, state: dict[str, Any]) -> None:
        object.__setattr__(self, "dag", state["dag"])
        object.__setattr__(self, "entry_node", state["entry_node"])
        object.__setattr__(self, "state", state.get("state"))
        object.__setattr__(self, "nodes", state.get("nodes"))
        object.__setattr__(self, "edges", state.get("edges"))
        object.__setattr__(self, "action_results", state.get("action_results"))
        instance_id = state.get("instance_id")
        if instance_id is None:
            instance_id = uuid4()
        object.__setattr__(self, "instance_id", instance_id)


@dataclass(frozen=True)
class InstanceDone:
    """Completed instance payload with result or exception."""

    executor_id: UUID
    entry_node: UUID
    result: Any | None
    error: Any | None


@dataclass(frozen=True)
class GraphUpdate:
    """Batch payload representing an updated execution graph snapshot.

    This intentionally stores only runtime nodes and edges (no DAG template or
    derived caches) so persistence stays lightweight.
    """

    instance_id: UUID
    nodes: "Mapping[UUID, ExecutionNode]"
    edges: "Iterable[ExecutionEdge]"


@dataclass(frozen=True)
class ActionDone:
    """Batch payload representing a completed action execution."""

    node_id: UUID
    action_name: str
    attempt: int
    result: Any


class BaseBackend(ABC):
    """Abstract persistence backend for runner state."""

    def batching(self) -> contextlib.AbstractContextManager["BaseBackend"]:
        """Return a context manager that batches related persistence calls."""
        return contextlib.nullcontext(self)

    @abstractmethod
    def save_graphs(self, graphs: Sequence[GraphUpdate]) -> None:
        """Persist updated execution graphs."""

    @abstractmethod
    def save_actions_done(self, actions: Sequence[ActionDone]) -> None:
        """Persist completed action executions."""

    @abstractmethod
    async def get_queued_instances(self, size: int) -> list["QueuedInstance"]:
        """Return up to size queued instances without blocking."""

    @abstractmethod
    def save_instances_done(self, instances: Sequence[InstanceDone]) -> None:
        """Persist completed workflow instances."""
