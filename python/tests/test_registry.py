from __future__ import annotations

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

    assert "sample" in action_registry.names()


def test_action_decorator_rejects_sync_functions() -> None:
    def sync_func() -> None:  # pragma: no cover - defined for decorator check
        return None

    with pytest.raises(TypeError):
        action(cast(Any, sync_func))
