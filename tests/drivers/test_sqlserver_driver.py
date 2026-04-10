"""SQL Server driver integration tests.

Spins up a real azure-sql-edge container (ARM64-compatible) via testcontainers,
pushes DbChangeEvents through the full process → flush pipeline, and verifies
persisted values.

Mirrors EDS.Integration.Tests.Drivers.SqlServerIntegrationTests.

Requirements:
  - Docker available
  - pyodbc installed
  - "ODBC Driver 18 for SQL Server" installed locally
    (macOS: brew install unixodbc && download ODBC driver from Microsoft)
"""

from __future__ import annotations

import asyncio
import uuid
from functools import partial

import pytest

from eds.drivers.sqlserver import SqlServerDriver
from tests.helpers.driver_helpers import (
    make_config, make_delete, make_insert, make_update, orders_schema,
)

pytestmark = pytest.mark.integration

_SCHEMA = orders_schema("eds_ss_orders")
_TABLE  = _SCHEMA.table


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _start_driver(sqlserver_url: str) -> SqlServerDriver:
    driver = SqlServerDriver()
    await driver.start(make_config(sqlserver_url))
    await driver.migrate_new_table(_SCHEMA)
    return driver


def _sync_query(conn_str: str, sql: str, params: tuple = ()):
    """Run a synchronous pyodbc query and return the first column of the first row."""
    import pyodbc  # type: ignore[import]
    conn = pyodbc.connect(conn_str, autocommit=True)
    try:
        cur = conn.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def _conn_str_from_url(url: str) -> str:
    """Convert a sqlserver:// driver URL to a raw pyodbc connection string."""
    from urllib.parse import urlparse, parse_qs
    u = urlparse(url)
    qs = parse_qs(u.query)
    db = (qs.get("database") or ["master"])[0]
    trust = (qs.get("trust-server-certificate") or ["true"])[0].lower() != "false"
    trust_str = "Yes" if trust else "No"
    return (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={u.hostname},{u.port or 1433};"
        f"DATABASE={db};"
        f"UID={u.username};PWD={u.password or ''};"
        f"TrustServerCertificate={trust_str};"
    )


async def _query_scalar(sqlserver_url: str, sql: str, params: tuple = ()):
    conn_str = _conn_str_from_url(sqlserver_url)
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, partial(_sync_query, conn_str, sql, params))


async def _row_exists(sqlserver_url: str, row_id: str) -> bool:
    safe_id = row_id.replace("'", "''")
    count = await _query_scalar(
        sqlserver_url,
        f"SELECT COUNT(*) FROM [{_TABLE}] WHERE id = '{safe_id}'",
    )
    return (count or 0) > 0


# ── Tests ─────────────────────────────────────────────────────────────────────

async def test_insert_basic_row_appears_in_database(sqlserver_url: str) -> None:
    row_id = str(uuid.uuid4())
    driver = await _start_driver(sqlserver_url)
    await driver.process(make_insert(_TABLE, row_id, {"id": row_id, "name": "Widget"}))
    await driver.flush()
    await driver.stop()

    assert await _row_exists(sqlserver_url, row_id)


async def test_insert_string_with_single_quote_stored_correctly(sqlserver_url: str) -> None:
    row_id = str(uuid.uuid4())
    name = "O'Brien's Shop"
    driver = await _start_driver(sqlserver_url)
    await driver.process(make_insert(_TABLE, row_id, {"id": row_id, "name": name}))
    await driver.flush()
    await driver.stop()

    stored = await _query_scalar(
        sqlserver_url,
        f"SELECT name FROM [{_TABLE}] WHERE id = '{row_id}'",
    )
    assert stored == name


async def test_insert_sql_injection_in_value_stored_literally(sqlserver_url: str) -> None:
    row_id = str(uuid.uuid4())
    injection = "'; DROP TABLE eds_ss_orders; --"
    driver = await _start_driver(sqlserver_url)
    await driver.process(make_insert(_TABLE, row_id, {"id": row_id, "name": injection}))
    await driver.flush()  # must not throw
    await driver.stop()

    # Table must still exist and the row must contain the injection string literally.
    stored = await _query_scalar(
        sqlserver_url,
        f"SELECT name FROM [{_TABLE}] WHERE id = '{row_id}'",
    )
    assert stored == injection


async def test_insert_unicode_value_stored_correctly(sqlserver_url: str) -> None:
    row_id = str(uuid.uuid4())
    name = "こんにちは 🎉 Ünïcödé"
    driver = await _start_driver(sqlserver_url)
    await driver.process(make_insert(_TABLE, row_id, {"id": row_id, "name": name}))
    await driver.flush()
    await driver.stop()

    stored = await _query_scalar(
        sqlserver_url,
        f"SELECT name FROM [{_TABLE}] WHERE id = '{row_id}'",
    )
    assert stored == name


async def test_insert_null_value_column_is_null_in_database(sqlserver_url: str) -> None:
    row_id = str(uuid.uuid4())
    driver = await _start_driver(sqlserver_url)
    await driver.process(make_insert(_TABLE, row_id, {"id": row_id, "name": None}))
    await driver.flush()
    await driver.stop()

    stored = await _query_scalar(
        sqlserver_url,
        f"SELECT name FROM [{_TABLE}] WHERE id = '{row_id}'",
    )
    assert stored is None


async def test_insert_numeric_values_stored_correctly(sqlserver_url: str) -> None:
    row_id = str(uuid.uuid4())
    driver = await _start_driver(sqlserver_url)
    await driver.process(make_insert(_TABLE, row_id, {"id": row_id, "amount": 123.45, "qty": 7}))
    await driver.flush()
    await driver.stop()

    amount_raw = await _query_scalar(sqlserver_url, f"SELECT amount FROM [{_TABLE}] WHERE id = '{row_id}'")
    qty_raw    = await _query_scalar(sqlserver_url, f"SELECT qty    FROM [{_TABLE}] WHERE id = '{row_id}'")
    assert float(amount_raw) == pytest.approx(123.45, rel=1e-4)
    assert int(qty_raw) == 7


async def test_insert_boolean_value_stored_correctly(sqlserver_url: str) -> None:
    row_id = str(uuid.uuid4())
    driver = await _start_driver(sqlserver_url)
    await driver.process(make_insert(_TABLE, row_id, {"id": row_id, "active": True}))
    await driver.flush()
    await driver.stop()

    stored = await _query_scalar(sqlserver_url, f"SELECT active FROM [{_TABLE}] WHERE id = '{row_id}'")
    assert stored is not None
    assert str(stored) in ("1", "true", "True")


async def test_update_existing_row_value_is_updated(sqlserver_url: str) -> None:
    row_id = str(uuid.uuid4())
    driver = await _start_driver(sqlserver_url)

    await driver.process(make_insert(_TABLE, row_id, {"id": row_id, "name": "Original"}))
    await driver.flush()

    await driver.process(make_update(_TABLE, row_id, {"id": row_id, "name": "Updated"}))
    await driver.flush()
    await driver.stop()

    stored = await _query_scalar(sqlserver_url, f"SELECT name FROM [{_TABLE}] WHERE id = '{row_id}'")
    assert stored == "Updated"


async def test_update_with_diff_only_named_column_changes(sqlserver_url: str) -> None:
    row_id = str(uuid.uuid4())
    driver = await _start_driver(sqlserver_url)

    await driver.process(make_insert(_TABLE, row_id, {"id": row_id, "name": "Alice", "amount": "100.0"}))
    await driver.flush()

    # Diff contains only "name" — amount in payload should be ignored.
    await driver.process(
        make_update(_TABLE, row_id, {"id": row_id, "name": "Bob", "amount": "999.0"}, diff=["name"])
    )
    await driver.flush()
    await driver.stop()

    name   = await _query_scalar(sqlserver_url, f"SELECT name   FROM [{_TABLE}] WHERE id = '{row_id}'")
    amount = await _query_scalar(sqlserver_url, f"SELECT amount FROM [{_TABLE}] WHERE id = '{row_id}'")
    assert name == "Bob"
    assert float(amount) == pytest.approx(100.0, rel=1e-2)  # unchanged


async def test_delete_existing_row_is_removed(sqlserver_url: str) -> None:
    row_id = str(uuid.uuid4())
    driver = await _start_driver(sqlserver_url)

    await driver.process(make_insert(_TABLE, row_id, {"id": row_id, "name": "To Delete"}))
    await driver.flush()
    assert await _row_exists(sqlserver_url, row_id)

    await driver.process(make_delete(_TABLE, row_id))
    await driver.flush()
    await driver.stop()

    assert not await _row_exists(sqlserver_url, row_id)


async def test_upsert_same_id_no_duplicate_row(sqlserver_url: str) -> None:
    row_id = str(uuid.uuid4())
    driver = await _start_driver(sqlserver_url)

    await driver.process(make_insert(_TABLE, row_id, {"id": row_id, "name": "First"}))
    await driver.flush()

    await driver.process(make_insert(_TABLE, row_id, {"id": row_id, "name": "Second"}))
    await driver.flush()
    await driver.stop()

    name  = await _query_scalar(sqlserver_url, f"SELECT name     FROM [{_TABLE}] WHERE id = '{row_id}'")
    count = await _query_scalar(sqlserver_url, f"SELECT COUNT(*) FROM [{_TABLE}] WHERE id = '{row_id}'")
    assert name == "Second"
    assert count == 1


async def test_multiple_batches_all_rows_committed(sqlserver_url: str) -> None:
    ids = [str(uuid.uuid4()) for _ in range(5)]
    driver = await _start_driver(sqlserver_url)

    for row_id in ids:
        await driver.process(make_insert(_TABLE, row_id, {"id": row_id, "name": f"row-{row_id}"}))

    await driver.flush()
    await driver.stop()

    for row_id in ids:
        assert await _row_exists(sqlserver_url, row_id), f"Row {row_id} not found"
