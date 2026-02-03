"""Backend interfaces for persisting runner state and action results."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Sequence, TYPE_CHECKING
from uuid import UUID

if TYPE_CHECKING:  # pragma: no cover - type checkers only
    from ..runner.state import RunnerState


@dataclass(frozen=True)
class GraphUpdate:
    """Batch payload representing an updated execution graph snapshot."""

    state: "RunnerState"


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
