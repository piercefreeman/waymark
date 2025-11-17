from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import inspect
from threading import RLock
from typing import Any, Optional, TypeVar

T = TypeVar("T", bound=Callable[..., Any])


@dataclass
class _ActionEntry:
    name: str
    func: Callable[..., Any]


class ActionRegistry:
    """In-memory registry of user-defined actions."""

    def __init__(self) -> None:
        self._actions: dict[str, _ActionEntry] = {}
        self._lock = RLock()

    def register(self, name: str, func: Callable[..., Any]) -> None:
        with self._lock:
            if name in self._actions:
                raise ValueError(f"action '{name}' already registered")
            self._actions[name] = _ActionEntry(name=name, func=func)

    def get(self, name: str) -> Optional[Callable[..., Any]]:
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


def action(func: Optional[T] = None, *, name: Optional[str] = None) -> Callable[[T], T] | T:
    """Decorator for registering callable actions."""

    def decorator(target: T) -> T:
        if not inspect.iscoroutinefunction(target):
            raise TypeError(f"action '{target.__name__}' must be defined with 'async def'")
        action_name = name or target.__name__
        registry.register(action_name, target)
        return target

    if func is not None:
        return decorator(func)
    return decorator
