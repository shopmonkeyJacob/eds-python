"""SQLite-backed key-value tracker and dead-letter queue.

Mirrors internal/tracker/tracker.go (backed by BuntDB in Go).
Uses stdlib sqlite3 — no external dependency required.
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TYPE_CHECKING

from eds.core.tracker import Tracker

if TYPE_CHECKING:
    from eds.core.models import DbChangeEvent


class SqliteTracker(Tracker):
    """Thread-safe, file-backed key-value store and dead-letter queue using SQLite."""

    def __init__(self, db_path: str | Path) -> None:
        self._path = str(db_path)
        self._conn: sqlite3.Connection | None = None
        self._lock = asyncio.Lock()

    async def open(self) -> None:
        async with self._lock:
            await asyncio.to_thread(self._open_sync)

    def _open_sync(self) -> None:
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS dlq (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                failed_at   TEXT    NOT NULL,
                event_id    TEXT    NOT NULL,
                table_name  TEXT    NOT NULL,
                operation   TEXT    NOT NULL,
                company_id  TEXT,
                location_id TEXT,
                retry_count INTEGER NOT NULL DEFAULT 0,
                error       TEXT    NOT NULL,
                payload     TEXT
            )
        """)
        self._conn.commit()

    async def get_key(self, key: str) -> str | None:
        assert self._conn, "Tracker not open"
        async with self._lock:
            return await asyncio.to_thread(self._get_key_sync, key)

    def _get_key_sync(self, key: str) -> str | None:
        assert self._conn
        row = self._conn.execute("SELECT value FROM kv WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    async def set_key(self, key: str, value: str) -> None:
        assert self._conn, "Tracker not open"
        async with self._lock:
            await asyncio.to_thread(self._set_key_sync, key, value)

    def _set_key_sync(self, key: str, value: str) -> None:
        assert self._conn
        self._conn.execute(
            "INSERT INTO kv (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self._conn.commit()

    async def delete_keys(self, *keys: str) -> None:
        assert self._conn, "Tracker not open"
        async with self._lock:
            await asyncio.to_thread(self._delete_keys_sync, keys)

    def _delete_keys_sync(self, keys: tuple[str, ...]) -> None:
        assert self._conn
        self._conn.executemany("DELETE FROM kv WHERE key = ?", [(k,) for k in keys])
        self._conn.commit()

    # ── Dead-letter queue ──────────────────────────────────────────────────────

    async def push_dlq(
        self,
        pending: list[tuple[Any, "DbChangeEvent", float]],
        error: str,
        retry_count: int = 0,
    ) -> None:
        """Persist *pending* events to the ``dlq`` table.

        Each row stores the event metadata and the raw After (or Before for
        deletes) JSON payload so operators can inspect or replay failed events.
        """
        if not pending or not self._conn:
            return

        async with self._lock:
            await asyncio.to_thread(self._push_dlq_sync, pending, error, retry_count)

    def _push_dlq_sync(
        self,
        pending: list[tuple[Any, "DbChangeEvent", float]],
        error: str,
        retry_count: int,
    ) -> None:
        assert self._conn
        failed_at   = datetime.now(timezone.utc).isoformat()
        error_trunc = error[:2000] if len(error) > 2000 else error

        cursor = self._conn.cursor()
        for _msg, evt, _received_ms in pending:
            payload: str | None = None
            if evt.after:
                try:
                    payload = evt.after.decode()
                except Exception:
                    payload = None
            elif evt.before:
                try:
                    payload = evt.before.decode()
                except Exception:
                    payload = None

            cursor.execute(
                """
                INSERT INTO dlq
                    (failed_at, event_id, table_name, operation,
                     company_id, location_id, retry_count, error, payload)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    failed_at,
                    evt.id,
                    evt.table,
                    evt.operation,
                    evt.company_id,
                    evt.location_id,
                    retry_count,
                    error_trunc,
                    payload,
                ),
            )
        self._conn.commit()

    async def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
