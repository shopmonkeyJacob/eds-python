"""MySQL driver integration tests.

Spins up a real MySQL container via testcontainers, pushes DbChangeEvents
through the full process → flush pipeline, and verifies persisted values.

Mirrors EDS.Integration.Tests.Drivers.MySqlIntegrationTests.
"""

from __future__ import annotations

import uuid
from urllib.parse import urlparse

import aiomysql  # type: ignore[import]
import pytest

from eds.drivers.mysql import MySQLDriver
from tests.helpers.driver_helpers import (
    make_config, make_delete, make_insert, make_update, orders_schema,
)

pytestmark = pytest.mark.integration

_SCHEMA = orders_schema("eds_mysql_orders")
_TABLE = _SCHEMA.table


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _start_driver(mysql_url: str) -> MySQLDriver:
    driver = MySQLDriver()
    await driver.start(make_config(mysql_url))
    await driver.migrate_new_table(_SCHEMA)
    return driver


async def _connect(mysql_url: str) -> aiomysql.Connection:
    u = urlparse(mysql_url)
    return await aiomysql.connect(
        host=u.hostname or "localhost",
        port=u.port or 3306,
        user=u.username or "",
        password=u.password or "",
        db=u.path.lstrip("/"),
        autocommit=True,
    )


async def _query_scalar(mysql_url: str, sql: str):
    conn = await _connect(mysql_url)
    try:
        async with conn.cursor() as cur:
            await cur.execute(sql)
            row = await cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


async def _row_exists(mysql_url: str, row_id: str) -> bool:
    safe_id = row_id.replace("'", "''")
    count = await _query_scalar(
        mysql_url,
        f"SELECT COUNT(*) FROM `{_TABLE}` WHERE id = '{safe_id}'",
    )
    return (count or 0) > 0


# ── Tests ─────────────────────────────────────────────────────────────────────

async def test_insert_basic_row_appears_in_database(mysql_url: str) -> None:
    row_id = str(uuid.uuid4())
    driver = await _start_driver(mysql_url)
    await driver.process(make_insert(_TABLE, row_id, {"id": row_id, "name": "Widget"}))
    await driver.flush()
    await driver.stop()

    assert await _row_exists(mysql_url, row_id)


async def test_insert_string_with_single_quote_stored_correctly(mysql_url: str) -> None:
    row_id = str(uuid.uuid4())
    name = "O'Brien's Shop"
    driver = await _start_driver(mysql_url)
    await driver.process(make_insert(_TABLE, row_id, {"id": row_id, "name": name}))
    await driver.flush()
    await driver.stop()

    stored = await _query_scalar(mysql_url, f"SELECT name FROM `{_TABLE}` WHERE id = '{row_id}'")
    assert stored == name


async def test_insert_sql_injection_in_value_stored_literally(mysql_url: str) -> None:
    row_id = str(uuid.uuid4())
    injection = "'; DROP TABLE eds_mysql_orders; --"
    driver = await _start_driver(mysql_url)
    await driver.process(make_insert(_TABLE, row_id, {"id": row_id, "name": injection}))
    await driver.flush()  # must not throw
    await driver.stop()

    stored = await _query_scalar(mysql_url, f"SELECT name FROM `{_TABLE}` WHERE id = '{row_id}'")
    assert stored == injection


async def test_insert_unicode_value_stored_correctly(mysql_url: str) -> None:
    row_id = str(uuid.uuid4())
    name = "こんにちは 🎉 Ünïcödé"
    driver = await _start_driver(mysql_url)
    await driver.process(make_insert(_TABLE, row_id, {"id": row_id, "name": name}))
    await driver.flush()
    await driver.stop()

    stored = await _query_scalar(mysql_url, f"SELECT name FROM `{_TABLE}` WHERE id = '{row_id}'")
    assert stored == name


async def test_insert_null_value_column_is_null_in_database(mysql_url: str) -> None:
    row_id = str(uuid.uuid4())
    driver = await _start_driver(mysql_url)
    await driver.process(make_insert(_TABLE, row_id, {"id": row_id, "name": None}))
    await driver.flush()
    await driver.stop()

    stored = await _query_scalar(mysql_url, f"SELECT name FROM `{_TABLE}` WHERE id = '{row_id}'")
    assert stored is None


async def test_insert_numeric_values_stored_correctly(mysql_url: str) -> None:
    row_id = str(uuid.uuid4())
    driver = await _start_driver(mysql_url)
    await driver.process(make_insert(_TABLE, row_id, {"id": row_id, "amount": 123.45, "qty": 7}))
    await driver.flush()
    await driver.stop()

    amount_raw = await _query_scalar(mysql_url, f"SELECT amount FROM `{_TABLE}` WHERE id = '{row_id}'")
    qty_raw    = await _query_scalar(mysql_url, f"SELECT qty    FROM `{_TABLE}` WHERE id = '{row_id}'")
    assert float(amount_raw) == pytest.approx(123.45, rel=1e-4)
    assert int(qty_raw) == 7


async def test_insert_boolean_value_stored_correctly(mysql_url: str) -> None:
    row_id = str(uuid.uuid4())
    driver = await _start_driver(mysql_url)
    await driver.process(make_insert(_TABLE, row_id, {"id": row_id, "active": True}))
    await driver.flush()
    await driver.stop()

    # MySQL stores Python bool True into a TEXT column as '1'.
    stored = await _query_scalar(mysql_url, f"SELECT active FROM `{_TABLE}` WHERE id = '{row_id}'")
    assert stored is not None
    assert str(stored) in ("1", "true", "True")


async def test_update_existing_row_value_is_updated(mysql_url: str) -> None:
    row_id = str(uuid.uuid4())
    driver = await _start_driver(mysql_url)

    await driver.process(make_insert(_TABLE, row_id, {"id": row_id, "name": "Original"}))
    await driver.flush()

    await driver.process(make_update(_TABLE, row_id, {"id": row_id, "name": "Updated"}))
    await driver.flush()
    await driver.stop()

    stored = await _query_scalar(mysql_url, f"SELECT name FROM `{_TABLE}` WHERE id = '{row_id}'")
    assert stored == "Updated"


async def test_delete_existing_row_is_removed(mysql_url: str) -> None:
    row_id = str(uuid.uuid4())
    driver = await _start_driver(mysql_url)

    await driver.process(make_insert(_TABLE, row_id, {"id": row_id, "name": "To Delete"}))
    await driver.flush()
    assert await _row_exists(mysql_url, row_id)

    await driver.process(make_delete(_TABLE, row_id))
    await driver.flush()
    await driver.stop()

    assert not await _row_exists(mysql_url, row_id)


async def test_upsert_same_id_no_duplicate_row(mysql_url: str) -> None:
    row_id = str(uuid.uuid4())
    driver = await _start_driver(mysql_url)

    await driver.process(make_insert(_TABLE, row_id, {"id": row_id, "name": "First"}))
    await driver.flush()

    await driver.process(make_insert(_TABLE, row_id, {"id": row_id, "name": "Second"}))
    await driver.flush()
    await driver.stop()

    name  = await _query_scalar(mysql_url, f"SELECT name     FROM `{_TABLE}` WHERE id = '{row_id}'")
    count = await _query_scalar(mysql_url, f"SELECT COUNT(*) FROM `{_TABLE}` WHERE id = '{row_id}'")
    assert name == "Second"
    assert count == 1


async def test_multiple_batches_all_rows_committed(mysql_url: str) -> None:
    ids = [str(uuid.uuid4()) for _ in range(5)]
    driver = await _start_driver(mysql_url)

    for row_id in ids:
        await driver.process(make_insert(_TABLE, row_id, {"id": row_id, "name": f"row-{row_id}"}))

    await driver.flush()
    await driver.stop()

    for row_id in ids:
        assert await _row_exists(mysql_url, row_id), f"Row {row_id} not found"
