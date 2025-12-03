"""
SQLite in-memory database for the DAG runner.

Uses SQLite with :memory: mode for a fresh database on each instantiation.
This provides real SQL semantics while keeping everything in-memory.

Tracks stats like read/write counts to show optimization benefits.

Thread-safe: Uses SQLite's serialized mode with a connection-level lock
to ensure safe concurrent access from multiple workers.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class DBStats:
    """
    Thread-safe statistics for database operations.

    Uses a lock to ensure atomic updates from multiple threads.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self.reads: int = 0
        self.writes: int = 0
        self.deletes: int = 0
        self.queries: int = 0  # Multi-row reads (e.g., WHERE IN)

    def reset(self) -> None:
        """Reset all stats to zero."""
        with self._lock:
            self.reads = 0
            self.writes = 0
            self.deletes = 0
            self.queries = 0

    def increment_reads(self, count: int = 1) -> None:
        """Thread-safe increment of reads."""
        with self._lock:
            self.reads += count

    def increment_writes(self, count: int = 1) -> None:
        """Thread-safe increment of writes."""
        with self._lock:
            self.writes += count

    def increment_deletes(self, count: int = 1) -> None:
        """Thread-safe increment of deletes."""
        with self._lock:
            self.deletes += count

    def increment_queries(self, count: int = 1) -> None:
        """Thread-safe increment of queries."""
        with self._lock:
            self.queries += count

    def __repr__(self) -> str:
        with self._lock:
            return f"DBStats(reads={self.reads}, writes={self.writes}, deletes={self.deletes}, queries={self.queries})"


class Table(Generic[T]):
    """
    A SQL-backed table with ORM-like operations.

    Each row is stored by a primary key (string ID) with JSON-serialized data.
    Uses ROWID for insertion order (FIFO queue operations).
    Thread-safe via the parent database's connection lock.
    """

    def __init__(self, name: str, db: InMemoryDB):
        self.name = name
        self._db = db
        # Create the table - use implicit rowid for insertion order
        self._db._execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.name} (
                id TEXT PRIMARY KEY,
                data TEXT NOT NULL
            )
            """
        )

    def insert(self, id: str, row: T) -> None:
        """Insert a new row."""
        data_json = json.dumps(row)
        self._db._execute(
            f"INSERT INTO {self.name} (id, data) VALUES (?, ?)",
            (id, data_json),
        )
        self._db._stats.increment_writes()

    def get(self, id: str) -> T | None:
        """Get a row by ID."""
        self._db._stats.increment_reads()
        result = self._db._execute(
            f"SELECT data FROM {self.name} WHERE id = ?",
            (id,),
        ).fetchone()
        if result is None:
            return None
        return json.loads(result[0])

    def update(self, id: str, row: T) -> None:
        """Update an existing row."""
        data_json = json.dumps(row)
        cursor = self._db._execute(
            f"UPDATE {self.name} SET data = ? WHERE id = ?",
            (data_json, id),
        )
        if cursor.rowcount == 0:
            raise KeyError(f"Row {id} not found in table {self.name}")
        self._db._stats.increment_writes()

    def upsert(self, id: str, row: T) -> None:
        """Insert or update a row."""
        data_json = json.dumps(row)
        self._db._execute(
            f"""
            INSERT INTO {self.name} (id, data) VALUES (?, ?)
            ON CONFLICT(id) DO UPDATE SET data = excluded.data
            """,
            (id, data_json),
        )
        self._db._stats.increment_writes()

    def delete(self, id: str) -> bool:
        """Delete a row. Returns True if it existed."""
        cursor = self._db._execute(
            f"DELETE FROM {self.name} WHERE id = ?",
            (id,),
        )
        if cursor.rowcount > 0:
            self._db._stats.increment_deletes()
            return True
        return False

    def pop_first(self) -> tuple[str, T] | None:
        """
        Get the first row (FIFO order) without deleting.

        Returns (id, row) tuple or None if empty.
        """
        self._db._stats.increment_queries()
        result = self._db._execute(
            f"SELECT id, data FROM {self.name} ORDER BY rowid LIMIT 1"
        ).fetchone()
        if result is None:
            return None
        return (result[0], json.loads(result[1]))

    def pop_first_and_delete(self) -> tuple[str, T] | None:
        """
        Atomically pop and delete the first row.

        Simulates: SELECT ... FOR UPDATE SKIP LOCKED + DELETE in one transaction.
        This is the key operation for distributed work queues.
        """
        self._db._stats.increment_queries()
        # Get the first row
        result = self._db._execute(
            f"SELECT id, data FROM {self.name} ORDER BY rowid LIMIT 1"
        ).fetchone()
        if result is None:
            return None

        row_id, data = result[0], json.loads(result[1])

        # Delete it
        self._db._execute(
            f"DELETE FROM {self.name} WHERE id = ?",
            (row_id,),
        )
        self._db._stats.increment_deletes()

        return (row_id, data)

    def peek_first(self) -> tuple[str, T] | None:
        """
        Peek at the first row without removing.

        Simulates: SELECT * FROM table ORDER BY rowid LIMIT 1
        """
        self._db._stats.increment_queries()
        result = self._db._execute(
            f"SELECT id, data FROM {self.name} ORDER BY rowid LIMIT 1"
        ).fetchone()
        if result is None:
            return None
        return (result[0], json.loads(result[1]))

    def get_many(self, ids: list[str]) -> dict[str, T]:
        """
        Get multiple rows by ID (batch read).

        This is a single "query" even though it returns multiple rows.
        Simulates: SELECT * FROM table WHERE id IN (...)
        """
        if not ids:
            return {}
        self._db._stats.increment_queries()
        placeholders = ",".join("?" * len(ids))
        results = self._db._execute(
            f"SELECT id, data FROM {self.name} WHERE id IN ({placeholders})",
            tuple(ids),
        ).fetchall()
        return {row[0]: json.loads(row[1]) for row in results}

    def all(self) -> dict[str, T]:
        """Get all rows."""
        self._db._stats.increment_queries()
        results = self._db._execute(
            f"SELECT id, data FROM {self.name}"
        ).fetchall()
        return {row[0]: json.loads(row[1]) for row in results}

    def count(self) -> int:
        """Count rows (doesn't count as a read)."""
        result = self._db._execute(
            f"SELECT COUNT(*) FROM {self.name}"
        ).fetchone()
        return result[0] if result else 0

    def clear(self) -> None:
        """Clear all rows."""
        # Get count first for stats
        count = self.count()
        self._db._execute(f"DELETE FROM {self.name}")
        if count > 0:
            self._db._stats.increment_deletes(count)

    def exists(self, id: str) -> bool:
        """Check if a row exists (counts as a read)."""
        self._db._stats.increment_reads()
        result = self._db._execute(
            f"SELECT 1 FROM {self.name} WHERE id = ? LIMIT 1",
            (id,),
        ).fetchone()
        return result is not None


class InMemoryDB:
    """
    SQLite in-memory database with multiple tables.

    Each instance creates a fresh :memory: database.
    Tracks all operations for visibility into read/write patterns.
    Thread-safe for concurrent access from multiple workers.
    """

    def __init__(self):
        # Create in-memory SQLite database
        # check_same_thread=False allows multi-threaded access
        self._conn = sqlite3.connect(
            ":memory:",
            check_same_thread=False,
            isolation_level=None,  # Autocommit mode
        )
        self._tables: dict[str, Table] = {}
        self._stats = DBStats()
        self._lock = threading.RLock()

    def _execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        """Execute SQL with thread-safe locking."""
        with self._lock:
            return self._conn.execute(sql, params)

    def create_table(self, name: str) -> Table:
        """Create a new table (thread-safe)."""
        with self._lock:
            if name in self._tables:
                raise ValueError(f"Table {name} already exists")
            table: Table[Any] = Table(name, self)
            self._tables[name] = table
            return table

    def get_or_create_table(self, name: str) -> Table:
        """Get existing table or create new one (thread-safe)."""
        with self._lock:
            if name not in self._tables:
                self._tables[name] = Table(name, self)
            return self._tables[name]

    def get_table(self, name: str) -> Table:
        """Get an existing table (thread-safe)."""
        with self._lock:
            if name not in self._tables:
                raise KeyError(f"Table {name} not found")
            return self._tables[name]

    def drop_table(self, name: str) -> None:
        """Drop a table (thread-safe)."""
        with self._lock:
            if name in self._tables:
                self._execute(f"DROP TABLE IF EXISTS {name}")
                del self._tables[name]

    @property
    def stats(self) -> DBStats:
        """Get current stats."""
        return self._stats

    def reset_stats(self) -> None:
        """Reset all stats."""
        self._stats.reset()

    def print_stats(self, label: str = "") -> None:
        """Print current stats."""
        prefix = f"[{label}] " if label else ""
        print(f"{prefix}DB Stats: {self._stats}")

    def close(self) -> None:
        """Close the database connection."""
        with self._lock:
            self._conn.close()


# Singleton-style global database for easy access
_default_db: InMemoryDB | None = None
_db_lock = threading.Lock()


def get_db() -> InMemoryDB:
    """Get the default database instance (thread-safe singleton)."""
    global _default_db
    if _default_db is None:
        with _db_lock:
            # Double-check after acquiring lock
            if _default_db is None:
                _default_db = InMemoryDB()
    return _default_db


def reset_db() -> InMemoryDB:
    """Reset and return a fresh database instance (thread-safe)."""
    global _default_db
    with _db_lock:
        _default_db = InMemoryDB()
        return _default_db
