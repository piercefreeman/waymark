"""Backend interfaces for persisting runner state and action results."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence, TYPE_CHECKING
from uuid import UUID

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
