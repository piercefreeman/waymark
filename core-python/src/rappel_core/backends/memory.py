"""In-memory backend that prints persistence operations."""

from __future__ import annotations

import asyncio
from typing import Optional, Sequence

from .base import ActionDone, BaseBackend, GraphUpdate, InstanceDone, QueuedInstance


class MemoryBackend(BaseBackend):
    """Backend that prints updates instead of persisting them."""

    def __init__(self, instance_queue: Optional["asyncio.Queue[QueuedInstance]"] = None) -> None:
        self._instance_queue = instance_queue

    def save_graphs(self, graphs: Sequence[GraphUpdate]) -> None:
        for graph in graphs:
            print(f"UPDATE {graph.instance_id} {graph}")

    def save_actions_done(self, actions: Sequence[ActionDone]) -> None:
        for action in actions:
            print(f"INSERT {action}")

    def save_instances_done(self, instances: Sequence[InstanceDone]) -> None:
        for instance in instances:
            print(f"INSERT {instance}")

    async def get_queued_instances(self, size: int) -> list[QueuedInstance]:
        if size <= 0 or self._instance_queue is None:
            return []
        instances: list[QueuedInstance] = []
        for _ in range(size):
            try:
                instances.append(self._instance_queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return instances
