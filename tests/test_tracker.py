"""Unit tests for the SQLite tracker and dead-letter queue."""

import json
import os
import sqlite3
import tempfile

import pytest

from eds.core.models import DbChangeEvent
from eds.infrastructure.tracker import SqliteTracker


@pytest.fixture
async def tracker():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    t = SqliteTracker(path)
    await t.open()
    yield t
    await t.close()
    os.unlink(path)


@pytest.fixture
async def tracker_with_path():
    """Yields (tracker, db_path) so tests can query the db directly."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    t = SqliteTracker(path)
    await t.open()
    yield t, path
    await t.close()
    os.unlink(path)


def _make_event(
    table: str = "orders",
    operation: str = "insert",
    company_id: str | None = "c1",
    location_id: str | None = "l1",
    after: str | None = None,
    before: str | None = None,
) -> DbChangeEvent:
    evt = DbChangeEvent(
        operation=operation,
        id="evt-" + table,
        table=table,
        key=["pk-1"],
        company_id=company_id,
        location_id=location_id,
    )
    if after is not None:
        evt.after = after.encode()
    if before is not None:
        evt.before = before.encode()
    return evt


def _read_dlq(path: str) -> list[dict]:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT event_id, table_name, operation, company_id, location_id, "
        "retry_count, error, payload FROM dlq ORDER BY id"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@pytest.mark.asyncio
async def test_set_and_get(tracker: SqliteTracker) -> None:
    await tracker.set_key("foo", "bar")
    val = await tracker.get_key("foo")
    assert val == "bar"


@pytest.mark.asyncio
async def test_get_missing_returns_none(tracker: SqliteTracker) -> None:
    val = await tracker.get_key("does-not-exist")
    assert val is None


@pytest.mark.asyncio
async def test_overwrite(tracker: SqliteTracker) -> None:
    await tracker.set_key("k", "v1")
    await tracker.set_key("k", "v2")
    val = await tracker.get_key("k")
    assert val == "v2"


@pytest.mark.asyncio
async def test_delete(tracker: SqliteTracker) -> None:
    await tracker.set_key("k", "v")
    await tracker.delete_keys("k")
    val = await tracker.get_key("k")
    assert val is None


@pytest.mark.asyncio
async def test_delete_multiple(tracker: SqliteTracker) -> None:
    await tracker.set_key("a", "1")
    await tracker.set_key("b", "2")
    await tracker.delete_keys("a", "b")
    assert await tracker.get_key("a") is None
    assert await tracker.get_key("b") is None


# ── push_dlq ──────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_push_dlq_single_event_all_fields(tracker_with_path: tuple) -> None:
    tracker, path = tracker_with_path
    evt = _make_event("orders", "insert", after='{"id":"e1","name":"Widget"}')
    pending = [(None, evt, 0.0)]

    await tracker.push_dlq(pending, "connection refused", retry_count=5)

    rows = _read_dlq(path)
    assert len(rows) == 1
    row = rows[0]
    assert row["event_id"]   == evt.id
    assert row["table_name"] == "orders"
    assert row["operation"]  == "insert"
    assert row["company_id"] == "c1"
    assert row["location_id"] == "l1"
    assert row["retry_count"] == 5
    assert row["error"]      == "connection refused"
    assert "Widget" in row["payload"]


@pytest.mark.asyncio
async def test_push_dlq_multiple_events_all_written(tracker_with_path: tuple) -> None:
    tracker, path = tracker_with_path
    pending = [
        (None, _make_event("invoices", "update", after=f'{{"id":"{i}"}}'), 0.0)
        for i in range(3)
    ]

    await tracker.push_dlq(pending, "timeout", retry_count=3)

    assert len(_read_dlq(path)) == 3


@pytest.mark.asyncio
async def test_push_dlq_null_optional_fields_stored_as_null(tracker_with_path: tuple) -> None:
    tracker, path = tracker_with_path
    evt = _make_event("services", "delete", company_id=None, location_id=None)

    await tracker.push_dlq([(None, evt, 0.0)], "error")

    row = _read_dlq(path)[0]
    assert row["company_id"]  is None
    assert row["location_id"] is None


@pytest.mark.asyncio
async def test_push_dlq_no_payload_stores_null(tracker_with_path: tuple) -> None:
    tracker, path = tracker_with_path
    evt = _make_event("parts", "delete")  # no after or before

    await tracker.push_dlq([(None, evt, 0.0)], "timeout")

    row = _read_dlq(path)[0]
    assert row["payload"] is None


@pytest.mark.asyncio
async def test_push_dlq_delete_event_falls_back_to_before(tracker_with_path: tuple) -> None:
    tracker, path = tracker_with_path
    evt = _make_event("parts", "delete", before='{"id":"b1","qty":5}')

    await tracker.push_dlq([(None, evt, 0.0)], "error", retry_count=2)

    row = _read_dlq(path)[0]
    assert "qty" in row["payload"]


@pytest.mark.asyncio
async def test_push_dlq_long_error_truncated_to_2000_chars(tracker_with_path: tuple) -> None:
    tracker, path = tracker_with_path
    long_error = "x" * 3000

    await tracker.push_dlq([(None, _make_event("orders", "insert"), 0.0)], long_error)

    row = _read_dlq(path)[0]
    assert len(row["error"]) == 2000


@pytest.mark.asyncio
async def test_push_dlq_empty_pending_writes_no_rows(tracker_with_path: tuple) -> None:
    tracker, path = tracker_with_path

    await tracker.push_dlq([], "error")

    assert _read_dlq(path) == []


@pytest.mark.asyncio
async def test_push_dlq_persists_across_reopen(tracker_with_path: tuple) -> None:
    tracker, path = tracker_with_path
    evt = _make_event("vehicles", "insert", after='{"id":"v1"}')
    await tracker.push_dlq([(None, evt, 0.0)], "disk full", retry_count=5)
    await tracker.close()

    t2 = SqliteTracker(path)
    await t2.open()
    try:
        rows = _read_dlq(path)
        assert len(rows) == 1
    finally:
        await t2.close()
