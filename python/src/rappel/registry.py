from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from threading import RLock
from typing import Any, Optional

AsyncAction = Callable[..., Awaitable[Any]]


@dataclass
class _ActionEntry:
    name: str
    func: AsyncAction


class ActionRegistry:
    """In-memory registry of user-defined actions."""

    def __init__(self) -> None:
        self._actions: dict[str, _ActionEntry] = {}
        self._lock = RLock()

    def register(self, name: str, func: AsyncAction) -> None:
        with self._lock:
            if name in self._actions:
                raise ValueError(f"action '{name}' already registered")
            self._actions[name] = _ActionEntry(name=name, func=func)

    def get(self, name: str) -> Optional[AsyncAction]:
        with self._lock:
            entry = self._actions.get(name)
            return entry.func if entry else None

    def names(self) -> list[str]:
        with self._lock:
            return sorted(self._actions.keys())

    def reset(self) -> None:
        with self._lock:
            self._actions.clear()


registry = ActionRegistry()
