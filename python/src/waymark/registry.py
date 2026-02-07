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

    def _source_fingerprint(self, func: AsyncAction) -> tuple[str | None, str | None]:
        func_any: Any = func
        try:
            code = func_any.__code__
        except AttributeError:
            return (None, None)
        try:
            qualname = func_any.__qualname__
        except AttributeError:
            qualname = None
        filename = code.co_filename
        if not isinstance(filename, str):
            filename = None
        if qualname is not None and not isinstance(qualname, str):
            qualname = None
        return (filename, qualname)

    def _is_same_action_definition(self, existing: AsyncAction, new: AsyncAction) -> bool:
        if existing is new:
            return True
        existing_fingerprint = self._source_fingerprint(existing)
        new_fingerprint = self._source_fingerprint(new)
        if existing_fingerprint == (None, None) or new_fingerprint == (None, None):
            return False
        return existing_fingerprint == new_fingerprint

    def register(self, module: str, name: str, func: AsyncAction) -> None:
        """Register an action with its module and name.

        Args:
            module: The Python module containing the action.
            name: The action name (from @action decorator).
            func: The async function to execute.

        Raises:
            ValueError: If an action with the same module:name is already registered
                with a different implementation.
        """
        key = _make_key(module, name)
        with self._lock:
            existing = self._actions.get(key)
            if existing is not None:
                if self._is_same_action_definition(existing.func, func):
                    self._actions[key] = _ActionEntry(module=module, name=name, func=func)
                    return
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

    def entries(self) -> list[_ActionEntry]:
        """Return all registered action entries."""
        with self._lock:
            return list(self._actions.values())

    def reset(self) -> None:
        """Clear all registered actions."""
        with self._lock:
            self._actions.clear()


registry = ActionRegistry()
