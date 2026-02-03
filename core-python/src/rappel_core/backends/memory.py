"""In-memory backend that prints persistence operations."""

from __future__ import annotations

from typing import Sequence

from .base import ActionDone, BaseBackend, GraphUpdate


class MemoryBackend(BaseBackend):
    """Backend that prints updates instead of persisting them."""

    def save_graphs(self, graphs: Sequence[GraphUpdate]) -> None:
        for graph in graphs:
            print(f"UPDATE {graph}")

    def save_actions_done(self, actions: Sequence[ActionDone]) -> None:
        for action in actions:
            print(f"INSERT {action}")
