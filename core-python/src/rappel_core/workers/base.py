"""Worker pool interface for executing actions."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Mapping
from uuid import UUID


@dataclass(frozen=True)
class ActionRequest:
    """Action execution request routed through the worker pool."""

    executor_id: UUID
    node_id: UUID
    action_name: str
    kwargs: Mapping[str, Any]


@dataclass(frozen=True)
class ActionCompletion:
    """Completed action result emitted by the worker pool."""

    executor_id: UUID
    node_id: UUID
    result: Any


class BaseWorkerPool(ABC):
    """Abstract worker pool with queue and batch completion polling."""

    @abstractmethod
    def queue(self, request: ActionRequest) -> None:
        """Submit an action request for execution."""

    @abstractmethod
    async def get_complete(self) -> list[ActionCompletion]:
        """Await and return a batch of completed actions."""
