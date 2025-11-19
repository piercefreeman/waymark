from __future__ import annotations

import pytest

from carabiner_worker.actions import action


def test_action_decorator_registers_async_function() -> None:
    @action(name="custom")
    async def sample() -> None:  # pragma: no cover
        raise NotImplementedError

    assert sample.__carabiner_action_name__ == "custom"
    assert sample.__carabiner_action_module__ == sample.__module__


def test_action_decorator_rejects_sync_functions() -> None:
    with pytest.raises(TypeError):

        @action
        def not_async() -> None:  # type: ignore[asyncio-not-allowed]
            raise NotImplementedError
