"""Unit tests for the SQLite tracker."""

import pytest
import tempfile
import os
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
