"""Postgres backend for persisting runner state and action results."""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import pickle
import sys
from collections import Counter
from threading import Lock
from typing import Any, Iterator, Sequence
from uuid import uuid4

from psycopg_pool import ConnectionPool

from .base import ActionDone, BaseBackend, GraphUpdate, InstanceDone, QueuedInstance

DEFAULT_DSN = "postgresql://rappel:rappel@localhost:5432/rappel_core"


def _ensure_proto_aliases() -> None:
    """Ensure protobuf modules are importable by their generated names."""
    if "ast_pb2" not in sys.modules:
        try:
            from proto import ast_pb2 as proto_ast
        except ImportError:
            proto_ast = None
        if proto_ast is not None:
            sys.modules["ast_pb2"] = proto_ast
    if "messages_pb2" not in sys.modules:
        try:
            from proto import messages_pb2 as proto_messages
        except ImportError:
            proto_messages = None
        if proto_messages is not None:
            sys.modules["messages_pb2"] = proto_messages


class PostgresBackend(BaseBackend):
    """Persist runner state and action results in Postgres."""

    _pool: ConnectionPool | None = None
    _pool_dsn: str | None = None
    _pool_lock = Lock()
    _schema_ready = False
    _schema_lock = Lock()

    def __init__(self, dsn: str = DEFAULT_DSN) -> None:
        _ensure_proto_aliases()
        self._dsn = dsn
        self._pool = self._get_pool(dsn)
        self._query_counts: Counter[str] = Counter()
        self._query_count_lock = Lock()
        self._batch_size_counts: dict[str, Counter[int]] = {}
        self._batch_size_lock = Lock()
        self._ensure_schema()

    def batching(self) -> contextlib.AbstractContextManager[BaseBackend]:
        return _postgres_batch(self)

    def save_graphs(self, graphs: Sequence[GraphUpdate]) -> None:
        if not graphs:
            return
        with self._pool.connection() as conn:
            self._update_graphs_in(conn, graphs)

    def save_actions_done(self, actions: Sequence[ActionDone]) -> None:
        if not actions:
            return
        payloads = [
            (
                action.node_id,
                action.action_name,
                action.attempt,
                self._serialize(action.result),
            )
            for action in actions
        ]
        self._copy_rows(
            "COPY runner_actions_done (node_id, action_name, attempt, result) FROM STDIN",
            payloads,
        )

    def save_instances_done(self, instances: Sequence[InstanceDone]) -> None:
        if not instances:
            return
        with self._pool.connection() as conn:
            self._update_instances_done_in(conn, instances)

    async def get_queued_instances(self, size: int) -> list[QueuedInstance]:
        if size <= 0:
            return []
        return await asyncio.to_thread(self._fetch_queued_instances, size)

    def queue_instances(self, instances: Sequence[QueuedInstance]) -> None:
        """Insert queued instances for run-loop consumption."""
        if not instances:
            return
        payloads: list[tuple[Any, Any]] = []
        instance_payloads: list[tuple[Any, Any, Any]] = []
        for instance in instances:
            instance_id = instance.instance_id
            if instance_id is None:
                instance_id = uuid4()
                instance = dataclasses.replace(instance, instance_id=instance_id)
            payloads.append((instance_id, self._serialize(instance)))
            graph = self._build_graph_update(instance, instance_id)
            instance_payloads.append(
                (
                    instance_id,
                    instance.entry_node,
                    self._serialize(graph),
                )
            )
        with self._pool.connection() as conn:
            with conn.transaction():
                self._copy_rows_in(
                    conn,
                    "COPY queued_instances (instance_id, payload) FROM STDIN",
                    payloads,
                )
                self._copy_rows_in(
                    conn,
                    "COPY runner_instances (instance_id, entry_node, state) FROM STDIN",
                    instance_payloads,
                )

    def clear_queue(self) -> None:
        """Delete all queued instances from the backing table."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                self._count_query("delete:queued_instances_all")
                cur.execute("DELETE FROM queued_instances")

    def clear_all(self) -> None:
        """Delete all persisted runner data for a clean benchmark run."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                self._count_query("truncate:runner_tables")
                cur.execute(
                    """
                    TRUNCATE runner_graph_updates,
                             runner_actions_done,
                             runner_instances,
                             runner_instances_done,
                             queued_instances
                    RESTART IDENTITY
                    """
                )

    def _fetch_queued_instances(self, size: int) -> list[QueuedInstance]:
        if size <= 0:
            return []
        with self._pool.connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    self._count_query("select:queued_instances")
                    cur.execute(
                        """
                        SELECT instance_id, payload
                        FROM queued_instances
                        ORDER BY created_at
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                        """,
                        (size,),
                    )
                    rows = cur.fetchall()
                    if not rows:
                        return []
                    self._count_batch_size("select:queued_instances", len(rows))
                    instance_ids = [row[0] for row in rows]
                    self._count_query("delete:queued_instances_by_id")
                    cur.execute(
                        "DELETE FROM queued_instances WHERE instance_id = ANY(%s)",
                        (instance_ids,),
                    )
        instances: list[QueuedInstance] = []
        for instance_id, payload in rows:
            instance = self._deserialize(payload)
            if instance.instance_id != instance_id:
                instance = dataclasses.replace(instance, instance_id=instance_id)
            instances.append(instance)
        return instances

    def _ensure_schema(self) -> None:
        if self.__class__._schema_ready:
            return
        with self.__class__._schema_lock:
            if self.__class__._schema_ready:
                return
            with self._pool.connection() as conn:
                with conn.cursor() as cur:
                    self._count_query("schema:runner_graph_updates")
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS runner_graph_updates (
                            id BIGSERIAL PRIMARY KEY,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            state BYTEA NOT NULL
                        )
                        """
                    )
                    self._count_query("schema:runner_actions_done")
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS runner_actions_done (
                            id BIGSERIAL PRIMARY KEY,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            node_id UUID NOT NULL,
                            action_name TEXT NOT NULL,
                            attempt INTEGER NOT NULL,
                            result BYTEA
                        )
                        """
                    )
                    self._count_query("schema:runner_instances")
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS runner_instances (
                            instance_id UUID PRIMARY KEY,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            entry_node UUID NOT NULL,
                            state BYTEA,
                            result BYTEA,
                            error BYTEA
                        )
                        """
                    )
                    self._count_query("schema:runner_instances_done")
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS runner_instances_done (
                            id BIGSERIAL PRIMARY KEY,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            executor_id UUID NOT NULL,
                            entry_node UUID NOT NULL,
                            result BYTEA,
                            error BYTEA
                        )
                        """
                    )
                    self._count_query("schema:queued_instances")
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS queued_instances (
                            instance_id UUID PRIMARY KEY,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            payload BYTEA NOT NULL
                        )
                        """
                    )
            self.__class__._schema_ready = True

    def _copy_rows(self, query: str, rows: Sequence[Sequence[Any]]) -> None:
        if not rows:
            return
        with self._pool.connection() as conn:
            self._copy_rows_in(conn, query, rows)

    def _copy_rows_in(self, conn: Any, query: str, rows: Sequence[Sequence[Any]]) -> None:
        if not rows:
            return
        self._count_query(self._copy_label(query))
        self._count_batch_size(self._copy_label(query), len(rows))
        with conn.cursor() as cur:
            with cur.copy(query) as copy:
                for row in rows:
                    copy.write_row(row)

    def _update_graphs_in(self, conn: Any, graphs: Sequence[GraphUpdate]) -> None:
        if not graphs:
            return
        self._count_query("update:runner_instances_state")
        self._count_batch_size("update:runner_instances_state", len(graphs))
        payloads = [(self._serialize(graph), graph.instance_id) for graph in graphs]
        with conn.cursor() as cur:
            cur.executemany(
                """
                UPDATE runner_instances
                SET state = %s
                WHERE instance_id = %s
                """,
                payloads,
            )

    def _update_instances_done_in(
        self, conn: Any, instances: Sequence[InstanceDone]
    ) -> None:
        if not instances:
            return
        self._count_query("update:runner_instances_result")
        self._count_batch_size("update:runner_instances_result", len(instances))
        payloads = [
            (
                self._serialize_optional(instance.result),
                self._serialize_optional(instance.error),
                instance.executor_id,
            )
            for instance in instances
        ]
        with conn.cursor() as cur:
            cur.executemany(
                """
                UPDATE runner_instances
                SET result = %s, error = %s
                WHERE instance_id = %s
                """,
                payloads,
            )

    def _build_graph_update(self, instance: QueuedInstance, instance_id: UUID) -> GraphUpdate:
        if instance.state is not None:
            nodes = instance.state.nodes
            edges = instance.state.edges
        else:
            nodes = instance.nodes
            edges = instance.edges
        if nodes is None or edges is None:
            raise ValueError("queued instance missing runner state for persistence")
        return GraphUpdate(instance_id=instance_id, nodes=nodes, edges=edges)

    @classmethod
    def _get_pool(cls, dsn: str) -> ConnectionPool:
        with cls._pool_lock:
            if cls._pool is None:
                cls._pool = ConnectionPool(conninfo=dsn, min_size=1, max_size=10)
                cls._pool_dsn = dsn
            elif cls._pool_dsn != dsn:
                raise ValueError("PostgresBackend DSN mismatch with global pool")
            return cls._pool

    @staticmethod
    def _serialize(value: Any) -> bytes:
        _ensure_proto_aliases()
        return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)

    @classmethod
    def _serialize_optional(cls, value: Any | None) -> bytes | None:
        if value is None:
            return None
        return cls._serialize(value)

    @staticmethod
    def _deserialize(payload: bytes | memoryview) -> QueuedInstance:
        _ensure_proto_aliases()
        if isinstance(payload, memoryview):
            payload = payload.tobytes()
        value = pickle.loads(payload)
        if not isinstance(value, QueuedInstance):
            raise TypeError("queued instance payload did not decode to QueuedInstance")
        return value

    def _count_query(self, label: str) -> None:
        with self._query_count_lock:
            self._query_counts[label] += 1

    def _count_batch_size(self, label: str, size: int) -> None:
        if size <= 0:
            return
        with self._batch_size_lock:
            counter = self._batch_size_counts.get(label)
            if counter is None:
                counter = Counter()
                self._batch_size_counts[label] = counter
            counter[size] += 1

    def _copy_label(self, query: str) -> str:
        parts = query.strip().split()
        if len(parts) >= 2:
            return f"copy:{parts[1]}"
        return "copy:unknown"

    def query_counts(self) -> dict[str, int]:
        with self._query_count_lock:
            return dict(self._query_counts)

    def batch_size_counts(self) -> dict[str, dict[int, int]]:
        with self._batch_size_lock:
            return {label: dict(counter) for label, counter in self._batch_size_counts.items()}


class _PostgresBatch(BaseBackend):
    def __init__(self, backend: PostgresBackend, conn: Any) -> None:
        self._backend = backend
        self._conn = conn

    def save_graphs(self, graphs: Sequence[GraphUpdate]) -> None:
        if not graphs:
            return
        self._backend._update_graphs_in(self._conn, graphs)

    def save_actions_done(self, actions: Sequence[ActionDone]) -> None:
        if not actions:
            return
        payloads = [
            (
                action.node_id,
                action.action_name,
                action.attempt,
                self._backend._serialize(action.result),
            )
            for action in actions
        ]
        self._backend._copy_rows_in(
            self._conn,
            "COPY runner_actions_done (node_id, action_name, attempt, result) FROM STDIN",
            payloads,
        )

    def save_instances_done(self, instances: Sequence[InstanceDone]) -> None:
        if not instances:
            return
        self._backend._update_instances_done_in(self._conn, instances)

    async def get_queued_instances(self, size: int) -> list[QueuedInstance]:
        return await self._backend.get_queued_instances(size)


@contextlib.contextmanager
def _postgres_batch(backend: PostgresBackend) -> Iterator[_PostgresBatch]:
    pool = backend._pool
    if pool is None:
        raise RuntimeError("Postgres connection pool not initialized")
    with pool.connection() as conn:
        with conn.transaction():
            backend._count_query("batch:transaction")
            yield _PostgresBatch(backend, conn)
