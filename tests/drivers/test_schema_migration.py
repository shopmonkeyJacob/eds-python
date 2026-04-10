"""Schema migration integration tests for PostgreSQL and MySQL.

Exercises migrate_changed_columns, migrate_removed_columns, and
drop_orphan_tables against real database containers.

Mirrors EDS.Integration.Tests.Drivers.SchemaMigrationTests.

Each test drops and recreates the target table so that it always starts with
the full schema, preventing one test from leaving the shared DB in a modified
state that would break precondition assertions in later tests.
"""

from __future__ import annotations

import uuid
from urllib.parse import urlparse

import asyncpg  # type: ignore[import]
import aiomysql  # type: ignore[import]
import pytest

from eds.drivers.mysql import MySQLDriver
from eds.drivers.postgres import PostgresDriver
from eds.core.models import SchemaColumn, TableSchema
from tests.helpers.driver_helpers import make_config, make_insert, orders_schema

pytestmark = pytest.mark.integration


# ── Shared schema builders ────────────────────────────────────────────────────

def _modified_schema(base: TableSchema, overrides: dict[str, str]) -> TableSchema:
    """Return a copy of *base* with the listed column data_types replaced."""
    new_cols = []
    for col in base.column_defs():
        if col.name in overrides:
            new_cols.append(SchemaColumn(
                name=col.name,
                data_type=overrides[col.name],
                nullable=col.nullable,
                primary_key=col.primary_key,
            ))
        else:
            new_cols.append(col)
    schema = TableSchema(table=base.table, model_version=base.model_version)
    schema._columns = new_cols
    return schema


def _reduced_schema(base: TableSchema, *drop_cols: str) -> TableSchema:
    """Return a copy of *base* with the named columns removed."""
    schema = TableSchema(table=base.table, model_version=base.model_version)
    schema._columns = [c for c in base.column_defs() if c.name not in drop_cols]
    return schema


# ══════════════════════════════════════════════════════════════════════════════
# PostgreSQL
# ══════════════════════════════════════════════════════════════════════════════

_PG_SCHEMA = orders_schema()          # table = "eds_test_orders"
_PG_TABLE  = _PG_SCHEMA.table


class TestPostgreSqlMigration:
    """Migration tests using the shared PostgreSQL container fixture."""

    # ── Per-test setup ────────────────────────────────────────────────────────

    async def _start_driver(self, postgres_url: str) -> PostgresDriver:
        # Drop the table first so each test starts with a clean, fully-columned schema.
        conn = await asyncpg.connect(postgres_url)
        try:
            await conn.execute(f'DROP TABLE IF EXISTS "{_PG_TABLE}" CASCADE')
        finally:
            await conn.close()

        driver = PostgresDriver()
        await driver.start(make_config(postgres_url))
        await driver.migrate_new_table(_PG_SCHEMA)
        return driver

    async def _query_scalar(self, postgres_url: str, sql: str):
        conn = await asyncpg.connect(postgres_url)
        try:
            return await conn.fetchval(sql)
        finally:
            await conn.close()

    async def _column_exists(self, postgres_url: str, column: str) -> bool:
        count = await self._query_scalar(
            postgres_url,
            f"SELECT COUNT(*) FROM information_schema.columns "
            f"WHERE table_name = '{_PG_TABLE}' AND column_name = '{column}'",
        )
        return (count or 0) > 0

    async def _table_exists(self, postgres_url: str, table: str) -> bool:
        count = await self._query_scalar(
            postgres_url,
            f"SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_name = '{table}'",
        )
        return (count or 0) > 0

    # ── MigrateChangedColumns ─────────────────────────────────────────────────

    async def test_migrate_changed_columns_does_not_throw_and_column_still_exists(
        self, postgres_url: str
    ) -> None:
        driver = await self._start_driver(postgres_url)

        # "qty" type change from text → something else (Python always alters to TEXT).
        new_schema = _modified_schema(_PG_SCHEMA, {"qty": "number"})
        await driver.migrate_changed_columns(new_schema, ["qty"])

        assert await self._column_exists(postgres_url, "qty")

    # ── MigrateRemovedColumns ─────────────────────────────────────────────────

    async def test_migrate_removed_columns_drops_column(self, postgres_url: str) -> None:
        driver = await self._start_driver(postgres_url)

        assert await self._column_exists(postgres_url, "name")

        reduced = _reduced_schema(_PG_SCHEMA, "name")
        await driver.migrate_removed_columns(reduced, ["name"])

        assert not await self._column_exists(postgres_url, "name")

    async def test_migrate_removed_columns_insert_without_dropped_column_succeeds(
        self, postgres_url: str
    ) -> None:
        driver = await self._start_driver(postgres_url)

        reduced = _reduced_schema(_PG_SCHEMA, "name")
        await driver.migrate_removed_columns(reduced, ["name"])

        # Insert a row without "name" using a fresh driver pointed at the reduced schema.
        row_id = str(uuid.uuid4())
        evt = make_insert(_PG_TABLE, row_id, {"id": row_id, "amount": "42.0"})

        driver2 = PostgresDriver()
        await driver2.start(make_config(postgres_url))
        await driver2.process(evt)
        await driver2.flush()  # must not throw
        await driver2.stop()

        count = await self._query_scalar(
            postgres_url,
            f"SELECT COUNT(*) FROM \"{_PG_TABLE}\" WHERE id = '{row_id}'",
        )
        assert count == 1

    # ── DropOrphanTables ──────────────────────────────────────────────────────

    async def test_drop_orphan_tables_drops_tables_not_in_known_set(
        self, postgres_url: str
    ) -> None:
        driver = await self._start_driver(postgres_url)

        # Create an orphan table directly.
        conn = await asyncpg.connect(postgres_url)
        try:
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS eds_pg_orphan_test (id TEXT PRIMARY KEY)"
            )
        finally:
            await conn.close()

        assert await self._table_exists(postgres_url, "eds_pg_orphan_test")

        await driver.drop_orphan_tables({_PG_TABLE})

        assert not await self._table_exists(postgres_url, "eds_pg_orphan_test")
        assert await self._table_exists(postgres_url, _PG_TABLE)

    async def test_drop_orphan_tables_when_all_known_does_not_drop_anything(
        self, postgres_url: str
    ) -> None:
        driver = await self._start_driver(postgres_url)

        assert await self._table_exists(postgres_url, _PG_TABLE)

        await driver.drop_orphan_tables({_PG_TABLE})

        assert await self._table_exists(postgres_url, _PG_TABLE)


# ══════════════════════════════════════════════════════════════════════════════
# MySQL
# ══════════════════════════════════════════════════════════════════════════════

_MY_SCHEMA = orders_schema("eds_mysql_migration")
_MY_TABLE  = _MY_SCHEMA.table


class TestMySqlMigration:
    """Migration tests using the shared MySQL container fixture."""

    # ── Per-test setup ────────────────────────────────────────────────────────

    async def _connect(self, mysql_url: str) -> aiomysql.Connection:
        u = urlparse(mysql_url)
        return await aiomysql.connect(
            host=u.hostname or "localhost",
            port=u.port or 3306,
            user=u.username or "",
            password=u.password or "",
            db=u.path.lstrip("/"),
            autocommit=True,
        )

    async def _start_driver(self, mysql_url: str) -> MySQLDriver:
        # Drop the table first so each test gets a clean, fully-columned schema.
        conn = await self._connect(mysql_url)
        try:
            async with conn.cursor() as cur:
                await cur.execute(f"DROP TABLE IF EXISTS `{_MY_TABLE}`")
        finally:
            conn.close()

        driver = MySQLDriver()
        await driver.start(make_config(mysql_url))
        await driver.migrate_new_table(_MY_SCHEMA)
        return driver

    async def _query_scalar(self, mysql_url: str, sql: str):
        conn = await self._connect(mysql_url)
        try:
            async with conn.cursor() as cur:
                await cur.execute(sql)
                row = await cur.fetchone()
                return row[0] if row else None
        finally:
            conn.close()

    async def _column_exists(self, mysql_url: str, column: str) -> bool:
        count = await self._query_scalar(
            mysql_url,
            f"SELECT COUNT(*) FROM information_schema.columns "
            f"WHERE table_name = '{_MY_TABLE}' AND column_name = '{column}' "
            f"AND table_schema = DATABASE()",
        )
        return (count or 0) > 0

    async def _column_data_type(self, mysql_url: str, column: str) -> str | None:
        return await self._query_scalar(
            mysql_url,
            f"SELECT DATA_TYPE FROM information_schema.columns "
            f"WHERE table_name = '{_MY_TABLE}' AND column_name = '{column}' "
            f"AND table_schema = DATABASE()",
        )

    async def _table_exists(self, mysql_url: str, table: str) -> bool:
        count = await self._query_scalar(
            mysql_url,
            f"SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_name = '{table}' AND table_schema = DATABASE()",
        )
        return (count or 0) > 0

    # ── MigrateChangedColumns ─────────────────────────────────────────────────

    async def test_migrate_changed_columns_does_not_throw_and_column_still_exists(
        self, mysql_url: str
    ) -> None:
        driver = await self._start_driver(mysql_url)

        new_schema = _modified_schema(_MY_SCHEMA, {"qty": "number"})
        await driver.migrate_changed_columns(new_schema, ["qty"])

        assert await self._column_exists(mysql_url, "qty")

    async def test_migrate_changed_columns_updates_column_data_type(
        self, mysql_url: str
    ) -> None:
        driver = await self._start_driver(mysql_url)

        # Python MySQL driver always modifies to TEXT (MODIFY COLUMN … TEXT).
        new_schema = _modified_schema(_MY_SCHEMA, {"qty": "number"})
        await driver.migrate_changed_columns(new_schema, ["qty"])

        data_type = await self._column_data_type(mysql_url, "qty")
        assert data_type is not None
        # MySQL TEXT → DATA_TYPE is 'text' (longtext after MODIFY COLUMN TEXT).
        assert data_type.lower() in ("text", "longtext", "mediumtext")

    # ── MigrateRemovedColumns ─────────────────────────────────────────────────

    async def test_migrate_removed_columns_drops_column(self, mysql_url: str) -> None:
        driver = await self._start_driver(mysql_url)

        assert await self._column_exists(mysql_url, "name")

        reduced = _reduced_schema(_MY_SCHEMA, "name")
        await driver.migrate_removed_columns(reduced, ["name"])

        assert not await self._column_exists(mysql_url, "name")

    async def test_migrate_removed_columns_insert_without_dropped_column_succeeds(
        self, mysql_url: str
    ) -> None:
        driver = await self._start_driver(mysql_url)

        reduced = _reduced_schema(_MY_SCHEMA, "name")
        await driver.migrate_removed_columns(reduced, ["name"])

        row_id = str(uuid.uuid4())
        evt = make_insert(_MY_TABLE, row_id, {"id": row_id, "amount": "42.0"})

        driver2 = MySQLDriver()
        await driver2.start(make_config(mysql_url))
        await driver2.process(evt)
        await driver2.flush()  # must not throw
        await driver2.stop()

        count = await self._query_scalar(
            mysql_url,
            f"SELECT COUNT(*) FROM `{_MY_TABLE}` WHERE id = '{row_id}'",
        )
        assert count == 1

    # ── DropOrphanTables ──────────────────────────────────────────────────────

    async def test_drop_orphan_tables_drops_tables_not_in_known_set(
        self, mysql_url: str
    ) -> None:
        driver = await self._start_driver(mysql_url)

        conn = await self._connect(mysql_url)
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    "CREATE TABLE IF NOT EXISTS eds_mysql_orphan_test (id VARCHAR(64) PRIMARY KEY)"
                )
        finally:
            conn.close()

        assert await self._table_exists(mysql_url, "eds_mysql_orphan_test")

        await driver.drop_orphan_tables({_MY_TABLE})

        assert not await self._table_exists(mysql_url, "eds_mysql_orphan_test")
        assert await self._table_exists(mysql_url, _MY_TABLE)

    async def test_drop_orphan_tables_when_all_known_does_not_drop_anything(
        self, mysql_url: str
    ) -> None:
        driver = await self._start_driver(mysql_url)

        assert await self._table_exists(mysql_url, _MY_TABLE)

        await driver.drop_orphan_tables({_MY_TABLE})

        assert await self._table_exists(mysql_url, _MY_TABLE)
