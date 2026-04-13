"""SQLite-backed key-value tracker.

Mirrors internal/tracker/tracker.go (backed by BuntDB in Go).
Uses stdlib sqlite3 — no external dependency required.
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

from eds.core.tracker import Tracker


class SqliteTracker(Tracker):
    """Thread-safe, file-backed key-value store using SQLite."""

    def __init__(self, db_path: str | Path) -> None:
        self._path = str(db_path)
        self._conn: sqlite3.Connection | None = None
        self._lock = asyncio.Lock()

    async def open(self) -> None:
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        self._conn.commit()

    async def get_key(self, key: str) -> str | None:
        assert self._conn, "Tracker not open"
        async with self._lock:
            row = self._conn.execute("SELECT value FROM kv WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    async def set_key(self, key: str, value: str) -> None:
        assert self._conn, "Tracker not open"
        async with self._lock:
            self._conn.execute(
                "INSERT INTO kv (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
            self._conn.commit()

    async def delete_keys(self, *keys: str) -> None:
        assert self._conn, "Tracker not open"
        async with self._lock:
            self._conn.executemany("DELETE FROM kv WHERE key = ?", [(k,) for k in keys])
            self._conn.commit()

    async def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
