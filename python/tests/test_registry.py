from __future__ import annotations

import pytest

from carabiner_worker import action, registry as action_registry


@pytest.fixture(autouse=True)
def reset_registry() -> None:
    action_registry.reset()
    yield
    action_registry.reset()


def test_action_decorator_accepts_async_functions() -> None:
    @action
    async def sample() -> str:
        return "ok"

    assert "sample" in action_registry.names()


def test_action_decorator_rejects_sync_functions() -> None:
    def sync_func() -> None:  # pragma: no cover - defined for decorator check
        return None

    with pytest.raises(TypeError):
        action(sync_func)
