"""PostgreSQL driver — uses asyncpg for async, parameterised queries."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse, parse_qs

import asyncpg  # type: ignore[import]

from eds.core.driver import (
    DriverField, FieldError,
    required_string, optional_string, optional_password, optional_number, get_str, get_int,
)
from eds.core.models import DbChangeEvent, TableSchema
from eds.drivers.base import SqlDriverBase


class PostgresDriver(SqlDriverBase):

    def __init__(self) -> None:
        super().__init__()
        self._pool: asyncpg.Pool | None = None

    # ── Identity ───────────────────────────────────────────────────────────────

    def name(self) -> str:
        return "PostgreSQL"

    def description(self) -> str:
        return "Stream CDC events into a PostgreSQL / CockroachDB database."

    def example_url(self) -> str:
        return "postgres://user:password@localhost:5432/mydb"

    def aliases(self) -> list[str]:
        return ["postgresql"]

    def _supports_upsert_migration(self) -> bool:
        return True

    # ── Configuration ─────────────────────────────────────────────────────────

    def configuration(self) -> list[DriverField]:
        return [
            required_string("Database", "Database name"),
            required_string("Hostname", "Host or IP address", "localhost"),
            optional_string("Username", "Database user"),
            optional_password("Password", "Database password"),
            optional_number("Port", "Port number", 5432),
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
        port = get_int(values, "Port", 5432)
        auth = f"{user}:{pwd}@" if user else ""
        return f"postgres://{auth}{host}:{port}/{db}", []

    # ── Connection ─────────────────────────────────────────────────────────────

    async def _connect(self, url: str) -> None:
        self._pool = await asyncpg.create_pool(url, min_size=1, max_size=5)

    async def _close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def test(self, url: str) -> None:
        conn = await asyncpg.connect(url)
        await conn.close()

    # ── Time-series dialect overrides ──────────────────────────────────────────

    def _build_create_or_replace_view_sql(self, qualified_view_name: str, select_sql: str) -> str:
        # PostgreSQL cannot add columns to a view via CREATE OR REPLACE — drop and recreate.
        return (
            f"DROP VIEW IF EXISTS {qualified_view_name} CASCADE;\n"
            f"CREATE VIEW {qualified_view_name} AS\n{select_sql}"
        )

    # ── DDL / event-insert execution ───────────────────────────────────────────

    async def _execute_ddl(self, sql: str) -> None:
        assert self._pool
        async with self._pool.acquire() as conn:
            await conn.execute(sql)

    async def _execute_insert_event(self, event: DbChangeEvent) -> None:
        assert self._pool
        qt, params = self._build_insert_event_params(event)
        sql = (
            f"INSERT INTO {qt} "
            "(_event_id, _operation, _entity_id, _timestamp, _mvcc_ts, "
            "_company_id, _location_id, _model_ver, _diff, _before, _after) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11::jsonb)"
        )
        async with self._pool.acquire() as conn:
            await conn.execute(sql, *params)

    # ── Upsert DML ────────────────────────────────────────────────────────────

    async def _execute_upsert(self, event: DbChangeEvent) -> None:
        assert self._pool
        obj = self._flatten_object(event)
        if not obj:
            return

        # For UPDATE with diff, only include changed columns plus the PK
        if event.operation == "UPDATE" and event.diff:
            cols = {k: v for k, v in obj.items() if k in event.diff or k == "id"}
        else:
            cols = obj

        columns = list(cols.keys())
        values = [self._coerce_value(cols[c]) for c in columns]
        placeholders = ", ".join(f"${i + 1}" for i in range(len(columns)))
        col_list = ", ".join(f'"{c}"' for c in columns)
        updates = ", ".join(
            f'"{c}" = EXCLUDED."{c}"' for c in columns if c != "id"
        )
        sql = (
            f'INSERT INTO "{event.table}" ({col_list}) VALUES ({placeholders}) '
            f'ON CONFLICT (id) DO UPDATE SET {updates}'
        )
        async with self._pool.acquire() as conn:
            await conn.execute(sql, *values)

    async def _execute_delete(self, event: DbChangeEvent) -> None:
        assert self._pool
        pk = event.get_primary_key()
        if not pk:
            return
        sql = f'DELETE FROM "{event.table}" WHERE id = $1'
        async with self._pool.acquire() as conn:
            await conn.execute(sql, pk)

    # ── Upsert schema migration ────────────────────────────────────────────────

    async def _migrate_new_table_upsert(self, schema: TableSchema) -> None:
        assert self._pool
        col_defs = []
        for col in schema.column_defs():
            null = "" if col.nullable else " NOT NULL"
            pk = " PRIMARY KEY" if col.primary_key else ""
            col_defs.append(f'  "{col.name}" TEXT{null}{pk}')
        ddl = f'CREATE TABLE IF NOT EXISTS "{schema.table}" (\n' + ",\n".join(col_defs) + "\n)"
        async with self._pool.acquire() as conn:
            await conn.execute(ddl)

    async def _migrate_new_columns_upsert(self, schema: TableSchema, columns: list[str]) -> None:
        assert self._pool
        col_map = {c.name: c for c in schema.column_defs()}
        async with self._pool.acquire() as conn:
            for col_name in columns:
                col = col_map.get(col_name)
                if col:
                    await conn.execute(
                        f'ALTER TABLE "{schema.table}" ADD COLUMN IF NOT EXISTS "{col.name}" TEXT'
                    )
