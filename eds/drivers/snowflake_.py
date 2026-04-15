"""Snowflake driver — uses snowflake-connector-python (sync) in an executor."""

from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import urlparse, parse_qs

import snowflake.connector

from eds.core.driver import (
    DriverField, FieldError,
    required_string, optional_string, optional_password, get_str,
)
from eds.core.models import DbChangeEvent, TableSchema
from eds.drivers.base import SqlDriverBase


class SnowflakeDriver(SqlDriverBase):

    def __init__(self) -> None:
        super().__init__()
        self._conn: Any = None
        self._loop: asyncio.AbstractEventLoop | None = None

    def name(self) -> str:
        return "Snowflake"

    def description(self) -> str:
        return "Stream CDC events into a Snowflake data warehouse."

    def example_url(self) -> str:
        return "snowflake://account/database/schema?warehouse=WH&role=ROLE"

    def configuration(self) -> list[DriverField]:
        return [
            required_string("Account", "Snowflake account identifier (e.g. myorg-myaccount)"),
            required_string("Database", "Database name"),
            required_string("Schema", "Schema name", "PUBLIC"),
            required_string("Warehouse", "Warehouse name"),
            optional_string("Username", "Snowflake user"),
            optional_password("Password", "Snowflake password"),
            optional_string("Role", "Snowflake role"),
        ]

    def validate(self, values: dict[str, Any]) -> tuple[str, list[FieldError]]:
        errors: list[FieldError] = []
        for f in ("Account", "Database", "Schema", "Warehouse"):
            if not get_str(values, f):
                errors.append(FieldError(f, f"{f} is required"))
        if errors:
            return "", errors
        account = get_str(values, "Account")
        db = get_str(values, "Database")
        schema = get_str(values, "Schema", "PUBLIC")
        wh = get_str(values, "Warehouse")
        user = get_str(values, "Username")
        pwd = get_str(values, "Password")
        role = get_str(values, "Role")
        auth = f"{user}:{pwd}@" if user else ""
        extra = f"&role={role}" if role else ""
        return f"snowflake://{auth}{account}/{db}/{schema}?warehouse={wh}{extra}", []

    async def _connect(self, url: str) -> None:
        self._loop = asyncio.get_event_loop()
        u = urlparse(url)
        qs = parse_qs(u.query)
        parts = u.path.lstrip("/").split("/")
        db = parts[0] if parts else ""
        schema = parts[1] if len(parts) > 1 else "PUBLIC"

        def _open() -> Any:
            return snowflake.connector.connect(
                account=u.hostname or "",
                user=u.username or "",
                password=u.password or "",
                database=db,
                schema=schema,
                warehouse=(qs.get("warehouse") or [""])[0],
                role=(qs.get("role") or [""])[0] or None,
            )

        self._conn = await self._loop.run_in_executor(None, _open)

    async def _close(self) -> None:
        if self._conn:
            conn = self._conn
            self._conn = None
            assert self._loop
            await self._loop.run_in_executor(None, conn.close)

    async def test(self, url: str) -> None:
        await self._connect(url)
        await self._close()

    async def _run(self, fn: Any) -> Any:
        assert self._loop
        return await self._loop.run_in_executor(None, fn)

    # ── Time-series dialect overrides ──────────────────────────────────────────

    def _auto_increment_pk_def(self) -> str:
        return "NUMBER AUTOINCREMENT PRIMARY KEY"

    def _json_column_type(self) -> str:
        return "VARIANT"

    def _json_extract(self, column: str, field: str) -> str:
        # Snowflake semi-structured: column:field::string
        return f'{column}:"{field}"::string'

    # ── Upsert schema migration ────────────────────────────────────────────────

    def _supports_upsert_migration(self) -> bool:
        return True

    async def _migrate_new_table_upsert(self, schema: TableSchema) -> None:
        col_defs = []
        for col in schema.column_defs():
            null = "" if col.nullable else " NOT NULL"
            pk = " PRIMARY KEY" if col.primary_key else ""
            col_defs.append(f'  "{col.name}" TEXT{null}{pk}')
        ddl = f'CREATE TABLE IF NOT EXISTS "{schema.table}" (\n' + ",\n".join(col_defs) + "\n)"
        await self._execute_ddl(ddl)

    async def _migrate_new_columns_upsert(self, schema: TableSchema, columns: list[str]) -> None:
        col_map = {c.name: c for c in schema.column_defs()}
        for col_name in columns:
            col = col_map.get(col_name)
            if col:
                await self._execute_ddl(
                    f'ALTER TABLE "{schema.table}" ADD COLUMN IF NOT EXISTS "{col.name}" TEXT'
                )

    # ── DDL / event-insert execution ───────────────────────────────────────────

    async def _execute_ddl(self, sql: str) -> None:
        assert self._conn

        def _exec() -> None:
            assert self._conn
            cur = self._conn.cursor()
            try:
                cur.execute(sql)
            finally:
                cur.close()

        await self._run(_exec)

    async def _execute_insert_event(self, event: DbChangeEvent) -> None:
        assert self._conn
        qt, params = self._build_insert_event_params(event)
        sql = (
            f"INSERT INTO {qt} "
            "(_event_id, _operation, _entity_id, _timestamp, _mvcc_ts, "
            "_company_id, _location_id, _model_ver, _diff, _before, _after) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), PARSE_JSON(%s))"
        )

        def _exec() -> None:
            assert self._conn
            cur = self._conn.cursor()
            try:
                cur.execute(sql, params)
            finally:
                cur.close()

        try:
            await self._run(_exec)
        except Exception:
            self._log_sql_error(sql, params)
            raise

    async def _execute_upsert(self, event: DbChangeEvent) -> None:
        assert self._conn
        obj = self._flatten_object(event)
        if not obj:
            return

        if event.operation == "UPDATE" and event.diff:
            cols = {k: v for k, v in obj.items() if k in event.diff or k == "id"}
        else:
            cols = obj

        columns = list(cols.keys())
        values = [self._coerce_value(cols[c]) for c in columns]
        non_pk = [c for c in columns if c != "id"]
        col_list = ", ".join(f'"{c}"' for c in columns)
        updates = ", ".join(f'"{c}" = src."{c}"' for c in non_pk)
        src_cols = ", ".join(f'src."{c}"' for c in columns)
        src_select = ", ".join("%s AS " + f'"{c}"' for c in columns)

        sql = (
            f'MERGE INTO "{event.table}" tgt '
            f"USING (SELECT {src_select}) src "
            f'ON tgt."id" = src."id" '
            f"WHEN MATCHED THEN UPDATE SET {updates} "
            f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({src_cols})"
        )

        def _exec() -> None:
            assert self._conn
            cur = self._conn.cursor()
            try:
                cur.execute(sql, values + values)
            finally:
                cur.close()

        try:
            await self._run(_exec)
        except Exception:
            self._log_sql_error(sql, values)
            raise

    async def _execute_delete(self, event: DbChangeEvent) -> None:
        assert self._conn
        pk = event.get_primary_key()
        if not pk:
            return

        def _exec() -> None:
            assert self._conn
            cur = self._conn.cursor()
            try:
                cur.execute(f'DELETE FROM "{event.table}" WHERE "id" = %s', (pk,))
            finally:
                cur.close()

        await self._run(_exec)
