from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from threading import RLock
from typing import Any, Optional

AsyncAction = Callable[..., Awaitable[Any]]


@dataclass
class _ActionEntry:
    module: str
    name: str
    func: AsyncAction


def _make_key(module: str, name: str) -> str:
    """Create a registry key from module and action name."""
    return f"{module}:{name}"


class ActionRegistry:
    """In-memory registry of user-defined actions.

    Actions are keyed by (module, name), allowing the same action name
    to be used in different modules.
    """

    def __init__(self) -> None:
        self._actions: dict[str, _ActionEntry] = {}
        self._lock = RLock()

    def register(self, module: str, name: str, func: AsyncAction) -> None:
        """Register an action with its module and name.

        Args:
            module: The Python module containing the action.
            name: The action name (from @action decorator).
            func: The async function to execute.

        Raises:
            ValueError: If an action with the same module:name is already registered.
        """
        key = _make_key(module, name)
        with self._lock:
            if key in self._actions:
                raise ValueError(f"action '{module}:{name}' already registered")
            self._actions[key] = _ActionEntry(module=module, name=name, func=func)

    def get(self, module: str, name: str) -> Optional[AsyncAction]:
        """Look up an action by module and name.

        Args:
            module: The Python module containing the action.
            name: The action name.

        Returns:
            The action function if found, None otherwise.
        """
        key = _make_key(module, name)
        with self._lock:
            entry = self._actions.get(key)
            return entry.func if entry else None

    def names(self) -> list[str]:
        """Return all registered action keys (module:name format)."""
        with self._lock:
            return sorted(self._actions.keys())

    def reset(self) -> None:
        """Clear all registered actions."""
        with self._lock:
            self._actions.clear()


registry = ActionRegistry()
