"""MySQL / MariaDB driver — uses aiomysql for async queries."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import urlparse

import aiomysql

from eds.core.driver import (
    DriverField, FieldError,
    required_string, optional_string, optional_password, optional_number, get_str, get_int,
)
from eds.core.models import DbChangeEvent, TableSchema
from eds.drivers.base import SqlDriverBase


class MySQLDriver(SqlDriverBase):

    def __init__(self) -> None:
        super().__init__()
        self._pool: aiomysql.Pool | None = None
        self._tx_conn: Any = None
        self._tx_cur: Any = None

    def name(self) -> str:
        return "MySQL"

    def description(self) -> str:
        return "Stream CDC events into a MySQL or MariaDB database."

    def example_url(self) -> str:
        return "mysql://user:password@localhost:3306/mydb"

    def _supports_upsert_migration(self) -> bool:
        return True

    def configuration(self) -> list[DriverField]:
        return [
            required_string("Database", "Database name"),
            required_string("Hostname", "Host or IP address", "localhost"),
            optional_string("Username", "Database user"),
            optional_password("Password", "Database password"),
            optional_number("Port", "Port number", 3306),
        ]

    def validate(self, values: dict[str, Any]) -> tuple[str, list[FieldError]]:
        errors: list[FieldError] = []
        db = get_str(values, "Database")
        host = get_str(values, "Hostname")
        if not db:
            errors.append(FieldError("Database", "Database is required"))
        if not host:
            errors.append(FieldError("Hostname", "Hostname is required"))
        if errors:
            return "", errors
        user = get_str(values, "Username")
        pwd = get_str(values, "Password")
        port = get_int(values, "Port", 3306)
        auth = f"{user}:{pwd}@" if user else ""
        return f"mysql://{auth}{host}:{port}/{db}", []

    async def _connect(self, url: str) -> None:
        u = urlparse(url)
        self._pool = await aiomysql.create_pool(
            host=u.hostname or "localhost",
            port=u.port or 3306,
            user=u.username or "",
            password=u.password or "",
            db=u.path.lstrip("/"),
            autocommit=True,
        )

    async def _close(self) -> None:
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None

    async def test(self, url: str) -> None:
        await self._connect(url)
        await self._close()

    @contextlib.asynccontextmanager
    async def _transaction(self) -> AsyncIterator[None]:
        assert self._pool
        async with self._pool.acquire() as conn:
            await conn.begin()
            cur = await conn.cursor()
            self._tx_conn = conn
            self._tx_cur = cur
            try:
                yield
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise
            finally:
                await cur.close()
                self._tx_conn = None
                self._tx_cur = None

    # ── Time-series dialect overrides ──────────────────────────────────────────

    def _ensure_events_schema_sql(self, schema_name: str) -> str:
        return ""  # MySQL has no schema concept within a database

    def _qualify_events_table(self, table: str) -> str:
        return f"`{table}__events`"

    def _qualify_events_view(self, view_name: str) -> str:
        return f"`{view_name}`"

    def _quote_id(self, name: str) -> str:
        return f"`{name}`"

    def _json_extract(self, column: str, field: str) -> str:
        return f"JSON_UNQUOTE(JSON_EXTRACT({column}, '$.{field}'))"

    def _auto_increment_pk_def(self) -> str:
        return "BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY"

    def _json_column_type(self) -> str:
        return "JSON"

    def _build_create_or_replace_view_sql(self, qualified_view_name: str, select_sql: str) -> str:
        return f"CREATE OR REPLACE VIEW {qualified_view_name} AS\n{select_sql}"

    # ── DDL / event-insert execution ───────────────────────────────────────────

    async def _execute_ddl(self, sql: str) -> None:
        assert self._pool
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)

    async def _execute_insert_event(self, event: DbChangeEvent) -> None:
        assert self._pool
        qt, params = self._build_insert_event_params(event)
        sql = (
            f"INSERT INTO {qt} "
            "(_event_id, _operation, _entity_id, _timestamp, _mvcc_ts, "
            "_company_id, _location_id, _model_ver, _diff, _before, _after) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        try:
            if self._tx_cur:
                await self._tx_cur.execute(sql, params)
            else:
                async with self._pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(sql, params)
        except Exception:
            self._log_sql_error(sql, params)
            raise

    # ── Upsert DML ────────────────────────────────────────────────────────────

    async def _execute_upsert(self, event: DbChangeEvent) -> None:
        assert self._pool
        obj = self._flatten_object(event)
        if not obj:
            return

        if event.operation == "UPDATE" and event.diff:
            cols = {k: v for k, v in obj.items() if k in event.diff or k == "id"}
        else:
            cols = obj

        columns = list(cols.keys())
        values = [self._coerce_value(cols[c]) for c in columns]
        col_list = ", ".join(f"`{c}`" for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        updates = ", ".join(f"`{c}` = VALUES(`{c}`)" for c in columns if c != "id")
        sql = (
            f"INSERT INTO `{event.table}` ({col_list}) VALUES ({placeholders}) "
            f"ON DUPLICATE KEY UPDATE {updates}"
        )
        try:
            if self._tx_cur:
                await self._tx_cur.execute(sql, values)
            else:
                async with self._pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(sql, values)
        except Exception:
            self._log_sql_error(sql, values)
            raise

    async def _execute_delete(self, event: DbChangeEvent) -> None:
        assert self._pool
        pk = event.get_primary_key()
        if not pk:
            return
        sql = f"DELETE FROM `{event.table}` WHERE id = %s"
        if self._tx_cur:
            await self._tx_cur.execute(sql, (pk,))
        else:
            async with self._pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(sql, (pk,))

    # ── Upsert schema migration ────────────────────────────────────────────────

    async def _migrate_new_table_upsert(self, schema: TableSchema) -> None:
        assert self._pool
        col_defs = []
        for col in schema.column_defs():
            null = "" if col.nullable else " NOT NULL"
            pk = " PRIMARY KEY" if col.primary_key else ""
            # MySQL requires a fixed-length type for PRIMARY KEY / indexed columns;
            # TEXT cannot be used as a key without a prefix length.
            col_type = "VARCHAR(64)" if col.primary_key else "TEXT"
            col_defs.append(f"  `{col.name}` {col_type}{null}{pk}")
        ddl = f"CREATE TABLE IF NOT EXISTS `{schema.table}` (\n" + ",\n".join(col_defs) + "\n)"
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(ddl)

    async def _migrate_new_columns_upsert(self, schema: TableSchema, columns: list[str]) -> None:
        assert self._pool
        col_map = {c.name: c for c in schema.column_defs()}
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                for col_name in columns:
                    col = col_map.get(col_name)
                    if col:
                        await cur.execute(
                            f"ALTER TABLE `{schema.table}` ADD COLUMN IF NOT EXISTS `{col.name}` TEXT"
                        )

    async def _migrate_changed_columns_upsert(self, schema: TableSchema, columns: list[str]) -> None:
        assert self._pool
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                for col_name in columns:
                    self._log.info("[mysql] Altering column type %s on %s.", col_name, schema.table)
                    await cur.execute(
                        f"ALTER TABLE `{schema.table}` MODIFY COLUMN `{col_name}` TEXT"
                    )

    async def _migrate_removed_columns_upsert(self, schema: TableSchema, columns: list[str]) -> None:
        assert self._pool
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                for col_name in columns:
                    self._log.info("[mysql] Dropping removed column %s from %s.", col_name, schema.table)
                    await cur.execute(
                        f"ALTER TABLE `{schema.table}` DROP COLUMN `{col_name}`"
                    )

    async def _drop_orphan_tables_upsert(self, known_tables: set[str]) -> None:
        assert self._pool
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT TABLE_NAME FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'"
                )
                rows = await cur.fetchall()
                existing = {row[0] for row in rows}
                orphans = existing - known_tables
                for table in orphans:
                    self._log.info("[mysql] Dropping orphan table %s.", table)
                    await cur.execute(f"DROP TABLE IF EXISTS `{table}`")
