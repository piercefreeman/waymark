"""
Simple in-memory database abstraction for the DAG runner.

This simulates what would be a real database backend, making it easy to
visualize the difference between inline execution (no DB roundtrip) vs
delegated execution (queue writes).

Tracks stats like read/write counts to show optimization benefits.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar
from copy import deepcopy

T = TypeVar("T")


@dataclass
class DBStats:
    """Statistics for database operations."""
    reads: int = 0
    writes: int = 0
    deletes: int = 0
    queries: int = 0  # Multi-row reads (e.g., WHERE IN)

    def reset(self) -> None:
        """Reset all stats to zero."""
        self.reads = 0
        self.writes = 0
        self.deletes = 0
        self.queries = 0

    def __repr__(self) -> str:
        return f"DBStats(reads={self.reads}, writes={self.writes}, deletes={self.deletes}, queries={self.queries})"


class Table(Generic[T]):
    """
    A simple in-memory table with ORM-like operations.

    Each row is stored by a primary key (string ID).
    Maintains insertion order for queue-like operations.
    """

    def __init__(self, name: str, db: InMemoryDB):
        self.name = name
        self._db = db
        self._rows: dict[str, T] = {}
        self._insertion_order: list[str] = []  # Track insertion order for FIFO

    def insert(self, id: str, row: T) -> None:
        """Insert a new row."""
        self._rows[id] = deepcopy(row)
        self._insertion_order.append(id)
        self._db._stats.writes += 1

    def get(self, id: str) -> T | None:
        """Get a row by ID."""
        self._db._stats.reads += 1
        row = self._rows.get(id)
        return deepcopy(row) if row else None

    def update(self, id: str, row: T) -> None:
        """Update an existing row."""
        if id not in self._rows:
            raise KeyError(f"Row {id} not found in table {self.name}")
        self._rows[id] = deepcopy(row)
        self._db._stats.writes += 1

    def upsert(self, id: str, row: T) -> None:
        """Insert or update a row."""
        is_new = id not in self._rows
        self._rows[id] = deepcopy(row)
        if is_new:
            self._insertion_order.append(id)
        self._db._stats.writes += 1

    def delete(self, id: str) -> bool:
        """Delete a row. Returns True if it existed."""
        if id in self._rows:
            del self._rows[id]
            if id in self._insertion_order:
                self._insertion_order.remove(id)
            self._db._stats.deletes += 1
            return True
        return False

    def pop_first(self) -> tuple[str, T] | None:
        """
        Pop the first row (FIFO order) - simulates SELECT ... LIMIT 1 FOR UPDATE SKIP LOCKED.

        Returns (id, row) tuple or None if empty.
        This is a read + delete in one atomic operation.
        """
        if not self._insertion_order:
            return None

        self._db._stats.queries += 1  # The SELECT query
        id = self._insertion_order[0]
        row = self._rows.get(id)
        if row is None:
            return None

        # Return copy, don't delete yet (caller decides)
        return (id, deepcopy(row))

    def peek_first(self) -> tuple[str, T] | None:
        """
        Peek at the first row without removing.

        Simulates: SELECT * FROM table ORDER BY created_at LIMIT 1
        """
        if not self._insertion_order:
            return None

        self._db._stats.queries += 1
        id = self._insertion_order[0]
        row = self._rows.get(id)
        if row is None:
            return None

        return (id, deepcopy(row))

    def get_many(self, ids: list[str]) -> dict[str, T]:
        """
        Get multiple rows by ID (batch read).

        This is a single "query" even though it returns multiple rows.
        Simulates: SELECT * FROM table WHERE id IN (...)
        """
        self._db._stats.queries += 1
        result = {}
        for id in ids:
            if id in self._rows:
                result[id] = deepcopy(self._rows[id])
        return result

    def all(self) -> dict[str, T]:
        """Get all rows."""
        self._db._stats.queries += 1
        return deepcopy(self._rows)

    def count(self) -> int:
        """Count rows (doesn't count as a read)."""
        return len(self._rows)

    def clear(self) -> None:
        """Clear all rows."""
        count = len(self._rows)
        self._rows.clear()
        self._insertion_order.clear()
        self._db._stats.deletes += count

    def exists(self, id: str) -> bool:
        """Check if a row exists (counts as a read)."""
        self._db._stats.reads += 1
        return id in self._rows


class InMemoryDB:
    """
    Simple in-memory database with multiple tables.

    Tracks all operations for visibility into read/write patterns.
    """

    def __init__(self):
        self._tables: dict[str, Table] = {}
        self._stats = DBStats()

    def create_table(self, name: str) -> Table:
        """Create a new table."""
        if name in self._tables:
            raise ValueError(f"Table {name} already exists")
        table: Table[Any] = Table(name, self)
        self._tables[name] = table
        return table

    def get_table(self, name: str) -> Table:
        """Get an existing table."""
        if name not in self._tables:
            raise KeyError(f"Table {name} not found")
        return self._tables[name]

    def drop_table(self, name: str) -> None:
        """Drop a table."""
        if name in self._tables:
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


# Singleton-style global database for easy access
_default_db: InMemoryDB | None = None


def get_db() -> InMemoryDB:
    """Get the default database instance."""
    global _default_db
    if _default_db is None:
        _default_db = InMemoryDB()
    return _default_db


def reset_db() -> InMemoryDB:
    """Reset and return a fresh database instance."""
    global _default_db
    _default_db = InMemoryDB()
    return _default_db
