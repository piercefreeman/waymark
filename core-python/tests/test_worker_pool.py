import asyncio
from dataclasses import dataclass
from typing import cast

from rappel_core.worker_pool import PythonWorkerConfig, PythonWorkerPool, WorkerBridgeServer


@dataclass
class FakeWorker:
    worker_id_value: int

    async def shutdown(self) -> None:
        return

    def worker_id(self) -> int:
        return self.worker_id_value


def test_worker_pool_acquire_release() -> None:
    async def run() -> None:
        next_id = 0

        async def factory(*_args, **_kwargs) -> FakeWorker:
            nonlocal next_id
            next_id += 1
            return FakeWorker(worker_id_value=next_id)

        config = PythonWorkerConfig.new()
        bridge = cast(WorkerBridgeServer, object())
        pool = await PythonWorkerPool.new_with_concurrency(
            config=config,
            count=2,
            bridge=bridge,
            max_action_lifecycle=None,
            max_concurrent_per_worker=1,
            worker_factory=factory,
        )

        idx = pool.try_acquire_slot()
        assert idx is not None
        assert pool.in_flight_for_worker(idx) == 1
        pool.release_slot(idx)
        assert pool.in_flight_for_worker(idx) == 0

    asyncio.run(run())
