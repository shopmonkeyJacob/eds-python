"""SQL Server driver — uses pyodbc (sync) wrapped in an executor thread."""

from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import urlparse, parse_qs

try:
    import pyodbc
except ImportError:
    pyodbc = None

from eds.core.driver import (
    DriverField, FieldError,
    required_string, optional_string, optional_password, optional_number,
    optional_string as opt_str, get_str, get_int,
)
from eds.core.models import DbChangeEvent, TableSchema
from eds.drivers.base import SqlDriverBase


class SqlServerDriver(SqlDriverBase):

    def __init__(self) -> None:
        super().__init__()
        self._conn: pyodbc.Connection | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    def name(self) -> str:
        return "SQL Server"

    def description(self) -> str:
        return "Stream CDC events into a Microsoft SQL Server database."

    def example_url(self) -> str:
        return "sqlserver://user:password@localhost:1433?database=mydb"

    def configuration(self) -> list[DriverField]:
        return [
            required_string("Database", "Database name"),
            required_string("Hostname", "Host or IP address", "localhost"),
            optional_string("Username", "Database user"),
            optional_password("Password", "Database password"),
            optional_number("Port", "Port number", 1433),
            opt_str("trust-server-certificate", "Set to 'false' to enforce TLS cert validation", "true"),
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
        port = get_int(values, "Port", 1433)
        trust = get_str(values, "trust-server-certificate", "true")
        auth = f"{user}:{pwd}@" if user else ""
        return f"sqlserver://{auth}{host}:{port}?database={db}&trust-server-certificate={trust}", []

    def _build_conn_str(self, url: str) -> str:
        u = urlparse(url)
        qs = parse_qs(u.query)
        db = (qs.get("database") or [u.path.lstrip("/")])[0]
        trust = (qs.get("trust-server-certificate") or ["true"])[0].lower() != "false"
        trust_str = "Yes" if trust else "No"
        driver = "{ODBC Driver 18 for SQL Server}"
        server = f"{u.hostname},{u.port or 1433}"
        if u.username:
            return (
                f"DRIVER={driver};SERVER={server};DATABASE={db};"
                f"UID={u.username};PWD={u.password or ''};"
                f"TrustServerCertificate={trust_str};"
            )
        return (
            f"DRIVER={driver};SERVER={server};DATABASE={db};"
            f"Trusted_Connection=Yes;TrustServerCertificate={trust_str};"
        )

    async def _connect(self, url: str) -> None:
        if pyodbc is None:
            raise ImportError("pyodbc is required for the SQL Server driver. "
                              "Install unixODBC then: pip install pyodbc")
        self._loop = asyncio.get_event_loop()
        conn_str = self._build_conn_str(url)
        self._conn = await self._loop.run_in_executor(
            None, lambda: pyodbc.connect(conn_str, autocommit=True)
        )

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

    def _ensure_events_schema_sql(self, schema_name: str) -> str:
        # CREATE SCHEMA must be the only statement in a batch — wrap in EXEC().
        escaped = schema_name.replace("'", "''")
        bracket = schema_name.replace("]", "]]")
        return (
            f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{escaped}') "
            f"EXEC(N'CREATE SCHEMA [{bracket}]')"
        )

    def _qualify_events_table(self, table: str) -> str:
        s = self._events_schema.replace("]", "]]")
        t = (table + "_events").replace("]", "]]")
        return f"[{s}].[{t}]"

    def _qualify_events_view(self, view_name: str) -> str:
        s = self._events_schema.replace("]", "]]")
        v = view_name.replace("]", "]]")
        return f"[{s}].[{v}]"

    def _quote_id(self, name: str) -> str:
        return f"[{name.replace(']', ']]')}]"

    def _json_extract(self, column: str, field: str) -> str:
        return f"JSON_VALUE({column}, N'$.{field}')"

    def _auto_increment_pk_def(self) -> str:
        return "BIGINT IDENTITY(1,1) PRIMARY KEY"

    def _json_column_type(self) -> str:
        return "NVARCHAR(MAX)"

    def _build_create_or_replace_view_sql(self, qualified_view_name: str, select_sql: str) -> str:
        # SQL Server: CREATE VIEW must be first in batch → use CREATE OR ALTER VIEW.
        return f"CREATE OR ALTER VIEW {qualified_view_name} AS\n{select_sql}"

    def _build_ensure_events_table_sql(self, table: str) -> str:
        # SQL Server does not support CREATE TABLE IF NOT EXISTS — use OBJECT_ID check.
        qt = self._qualify_events_table(table)
        full_name = f"{self._events_schema}.{table}_events"
        escaped = full_name.replace("'", "''")
        return (
            f"IF OBJECT_ID(N'{escaped}', N'U') IS NULL\n"
            f"BEGIN\n"
            f"CREATE TABLE {qt} (\n"
            f"  _seq         BIGINT IDENTITY(1,1) PRIMARY KEY,\n"
            f"  _event_id    NVARCHAR(MAX),\n"
            f"  _operation   NVARCHAR(MAX) NOT NULL,\n"
            f"  _entity_id   NVARCHAR(MAX),\n"
            f"  _timestamp   BIGINT,\n"
            f"  _mvcc_ts     NVARCHAR(MAX),\n"
            f"  _company_id  NVARCHAR(MAX),\n"
            f"  _location_id NVARCHAR(MAX),\n"
            f"  _model_ver   NVARCHAR(MAX),\n"
            f"  _diff        NVARCHAR(MAX),\n"
            f"  _before      NVARCHAR(MAX),\n"
            f"  _after       NVARCHAR(MAX)\n"
            f")\n"
            f"END"
        )

    # ── DDL / event-insert execution ───────────────────────────────────────────

    async def _execute_ddl(self, sql: str) -> None:
        assert self._conn

        def _exec() -> None:
            assert self._conn
            self._conn.execute(sql)

        await self._run(_exec)

    async def _execute_insert_event(self, event: DbChangeEvent) -> None:
        assert self._conn
        qt, params = self._build_insert_event_params(event)
        sql = (
            f"INSERT INTO {qt} "
            "(_event_id, _operation, _entity_id, _timestamp, _mvcc_ts, "
            "_company_id, _location_id, _model_ver, _diff, _before, _after) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )

        def _exec() -> None:
            assert self._conn
            self._conn.execute(sql, params)

        await self._run(_exec)

    # ── Upsert schema migration ────────────────────────────────────────────────

    def _supports_upsert_migration(self) -> bool:
        return True

    async def _migrate_new_table_upsert(self, schema: TableSchema) -> None:
        col_defs = []
        for col in schema.column_defs():
            null = "" if col.nullable else " NOT NULL"
            pk = " PRIMARY KEY" if col.primary_key else ""
            col_defs.append(f"  {self._quote_id(col.name)} NVARCHAR(MAX){null}{pk}")
        escaped = schema.table.replace("'", "''")
        quoted = self._quote_id(schema.table)
        ddl = (
            f"IF OBJECT_ID(N'{escaped}', N'U') IS NULL\n"
            f"BEGIN\n"
            f"CREATE TABLE {quoted} (\n"
            + ",\n".join(col_defs) + "\n"
            + ")\nEND"
        )
        await self._execute_ddl(ddl)

    async def _migrate_new_columns_upsert(self, schema: TableSchema, columns: list[str]) -> None:
        col_map = {c.name: c for c in schema.column_defs()}
        for col_name in columns:
            col = col_map.get(col_name)
            if col:
                await self._execute_ddl(
                    f"ALTER TABLE {self._quote_id(schema.table)} "
                    f"ADD {self._quote_id(col.name)} NVARCHAR(MAX) NULL"
                )

    async def _migrate_changed_columns_upsert(self, schema: TableSchema, columns: list[str]) -> None:
        for col_name in columns:
            self._log.info("[sqlserver] Altering column type %s on %s.", col_name, schema.table)
            await self._execute_ddl(
                f"ALTER TABLE {self._quote_id(schema.table)} "
                f"ALTER COLUMN {self._quote_id(col_name)} NVARCHAR(MAX) NULL"
            )

    async def _migrate_removed_columns_upsert(self, schema: TableSchema, columns: list[str]) -> None:
        for col_name in columns:
            self._log.info("[sqlserver] Dropping removed column %s from %s.", col_name, schema.table)
            await self._execute_ddl(
                f"ALTER TABLE {self._quote_id(schema.table)} "
                f"DROP COLUMN {self._quote_id(col_name)}"
            )

    async def _drop_orphan_tables_upsert(self, known_tables: set[str]) -> None:
        assert self._conn

        def _fetch() -> list[str]:
            assert self._conn
            cur = self._conn.execute(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_TYPE = 'BASE TABLE'"
            )
            return [row[0] for row in cur.fetchall()]

        existing = set(await self._run(_fetch))
        orphans = existing - known_tables
        for table in orphans:
            self._log.info("[sqlserver] Dropping orphan table %s.", table)
            escaped = table.replace("'", "''")
            await self._execute_ddl(
                f"IF OBJECT_ID(N'{escaped}', N'U') IS NOT NULL "
                f"DROP TABLE {self._quote_id(table)}"
            )

    # ── Upsert DML ────────────────────────────────────────────────────────────

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
        non_pk_cols = [c for c in columns if c != "id"]
        col_list = ", ".join(f"[{c}]" for c in columns)
        src_list = ", ".join(f"src.[{c}]" for c in columns)
        placeholders = ", ".join(["?"] * len(columns))
        updates = ", ".join(f"tgt.[{c}] = src.[{c}]" for c in non_pk_cols)

        sql = (
            f"MERGE [{event.table}] AS tgt "
            f"USING (VALUES ({placeholders})) AS src ({col_list}) "
            f"ON tgt.[id] = src.[id] "
            f"WHEN MATCHED THEN UPDATE SET {updates} "
            f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({src_list});"
        )

        def _exec() -> None:
            assert self._conn
            self._conn.execute(sql, values)

        await self._run(_exec)

    async def _execute_delete(self, event: DbChangeEvent) -> None:
        assert self._conn
        pk = event.get_primary_key()
        if not pk:
            return

        def _exec() -> None:
            assert self._conn
            self._conn.execute(f"DELETE FROM [{event.table}] WHERE [id] = ?", (pk,))

        await self._run(_exec)
