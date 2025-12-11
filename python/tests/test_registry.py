from collections.abc import Iterator
from typing import Any, cast

import pytest

from rappel import action
from rappel import registry as action_registry


@pytest.fixture(autouse=True)
def reset_registry() -> Iterator[None]:
    action_registry.reset()
    yield
    action_registry.reset()


def test_action_decorator_accepts_async_functions() -> None:
    @action
    async def sample() -> str:
        return "ok"

    # Registry keys are now module:name format
    assert f"{__name__}:sample" in action_registry.names()


def test_action_decorator_rejects_sync_functions() -> None:
    def sync_func() -> None:  # pragma: no cover - defined for decorator check
        return None

    with pytest.raises(TypeError):
        action(cast(Any, sync_func))


def test_registry_allows_same_name_different_modules() -> None:
    """Test that actions with the same name can coexist in different modules."""

    async def action_one() -> str:
        return "from module_a"

    async def action_two() -> str:
        return "from module_b"

    # Register same action name under different modules
    action_registry.register("module_a", "process", action_one)
    action_registry.register("module_b", "process", action_two)

    # Both should be retrievable independently
    handler_a = action_registry.get("module_a", "process")
    handler_b = action_registry.get("module_b", "process")

    assert handler_a is action_one
    assert handler_b is action_two
    assert handler_a is not handler_b

    # Names should show both registrations
    names = action_registry.names()
    assert "module_a:process" in names
    assert "module_b:process" in names


def test_registry_rejects_duplicate_in_same_module() -> None:
    """Test that duplicate action names in the same module raise an error."""

    async def action_one() -> str:
        return "first"

    async def action_two() -> str:
        return "second"

    action_registry.register("my_module", "duplicate", action_one)

    with pytest.raises(ValueError, match="my_module:duplicate.*already registered"):
        action_registry.register("my_module", "duplicate", action_two)


def test_registry_get_returns_none_for_unknown() -> None:
    """Test that get returns None for unregistered actions."""
    assert action_registry.get("unknown_module", "unknown_action") is None
