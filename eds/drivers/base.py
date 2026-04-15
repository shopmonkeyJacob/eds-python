"""SQL driver base class — mirrors the shared SQL logic in the .NET SqlDriverBase
and the Go internal/drivers SQL patterns.

Provides:
- Shared connection lifecycle management
- Upsert-mode: MERGE/UPSERT and DELETE SQL generation (delegated to subclasses)
- Time-series mode: fixed-schema events table INSERT + auto-maintained views
- Dialect hook methods for subclass customisation (quoting, JSON extraction, DDL syntax)
"""

from __future__ import annotations

import json
import logging
import re
from abc import abstractmethod
from typing import Any

from eds.core.driver import Driver, DriverConfig, DriverMode
from eds.core.models import DbChangeEvent, TableSchema

# Pattern that all Shopmonkey table/column names must match.
# Restricts to ASCII word characters only — no SQL metacharacters, quotes,
# semicolons, or other characters that could escape quoting contexts.
_SAFE_IDENTIFIER_RE = re.compile(r'^[A-Za-z0-9_]{1,128}$')


def _assert_safe_identifier(name: str) -> None:
    """Raise ValueError if *name* is not a safe SQL identifier.

    All table and column names that originate from the NATS wire (event.table,
    schema column names) are passed through this guard before being interpolated
    into any SQL string.  This prevents a compromised or spoofed NATS stream
    from injecting arbitrary SQL through a crafted table name.
    """
    if not _SAFE_IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Unsafe SQL identifier rejected: {name!r}. "
            "Identifiers must contain only ASCII letters, digits, and underscores."
        )


class SqlDriverBase(Driver):
    """Abstract base for SQL (relational) EDS drivers."""

    def __init__(self) -> None:
        self._log: logging.Logger = logging.getLogger(self.__class__.__name__)
        self._config: DriverConfig | None = None
        self._pending: list[DbChangeEvent] = []
        self._mode: DriverMode = DriverMode.UPSERT
        self._events_schema: str = "eds_events"

    # ── Driver lifecycle ───────────────────────────────────────────────────────

    @abstractmethod
    async def _connect(self, url: str) -> None:
        """Open the database connection."""

    @abstractmethod
    async def _close(self) -> None:
        """Close the database connection."""

    @abstractmethod
    async def _execute_upsert(self, event: DbChangeEvent) -> None:
        """Execute an INSERT … ON CONFLICT … DO UPDATE (or dialect equivalent)."""

    @abstractmethod
    async def _execute_delete(self, event: DbChangeEvent) -> None:
        """Execute a DELETE by primary key."""

    @abstractmethod
    async def _execute_ddl(self, sql: str) -> None:
        """Execute a single DDL statement (CREATE TABLE / CREATE VIEW / etc.)."""

    @abstractmethod
    async def _execute_insert_event(self, event: DbChangeEvent) -> None:
        """Execute a time-series INSERT into the events table (parameterised)."""

    async def start(self, config: DriverConfig) -> None:
        self._config = config
        self._log = config.logger
        self._mode = config.mode
        self._events_schema = config.events_schema
        await self._connect(config.url)

    async def stop(self) -> None:
        await self.flush()
        await self._close()

    # ── Batching ───────────────────────────────────────────────────────────────

    def max_batch_size(self) -> int:
        return -1  # no hard limit; rely on latency timers

    async def process(self, event: DbChangeEvent) -> bool:
        self._pending.append(event)
        return False

    async def flush(self) -> None:
        if not self._pending:
            return
        for evt in self._pending:
            _assert_safe_identifier(evt.table)
            # Imported events (bulk import snapshot) always go to the standard mirror
            # table via upsert, regardless of driver mode. Time-series events tables
            # are populated by the live CDC stream once the server starts.
            if self._mode == DriverMode.TIMESERIES and not evt.imported:
                await self._execute_insert_event(evt)
            elif evt.operation == "DELETE":
                await self._execute_delete(evt)
            else:
                await self._execute_upsert(evt)
        self._pending = []

    async def test(self, url: str) -> None:
        await self._connect(url)
        await self._close()

    # ── Migration ─────────────────────────────────────────────────────────────

    def supports_migration(self) -> bool:
        # In time-series mode all SQL drivers can create fixed-schema events tables.
        if self._mode == DriverMode.TIMESERIES:
            return True
        return self._supports_upsert_migration()

    def _supports_upsert_migration(self) -> bool:
        """Override in subclasses that support upsert-mode schema migration."""
        return False

    async def migrate_new_table(self, schema: TableSchema) -> None:
        _assert_safe_identifier(schema.table)
        for col in schema.column_defs():
            _assert_safe_identifier(col.name)
        if self._mode == DriverMode.TIMESERIES:
            await self._ensure_events_table_and_views(schema.table, schema)
        else:
            await self._migrate_new_table_upsert(schema)

    async def migrate_new_columns(self, schema: TableSchema, columns: list[str]) -> None:
        _assert_safe_identifier(schema.table)
        for col in columns:
            _assert_safe_identifier(col)
        if self._mode == DriverMode.TIMESERIES:
            # Events table schema is fixed — just refresh all three views.
            await self._execute_ddl(self._build_current_view_sql(schema.table))
            await self._execute_ddl(self._build_history_view_sql(schema.table, schema))
            await self._try_create_unified_view(schema.table, schema)
        else:
            await self._migrate_new_columns_upsert(schema, columns)

    async def migrate_changed_columns(self, schema: TableSchema, columns: list[str]) -> None:
        """Alter the type of columns whose data_type changed in the HQ schema."""
        _assert_safe_identifier(schema.table)
        for col in columns:
            _assert_safe_identifier(col)
        if self._mode == DriverMode.TIMESERIES:
            return  # events table schema is fixed
        await self._migrate_changed_columns_upsert(schema, columns)

    async def migrate_removed_columns(self, schema: TableSchema, columns: list[str]) -> None:
        """Drop columns that were removed from the HQ schema."""
        _assert_safe_identifier(schema.table)
        for col in columns:
            _assert_safe_identifier(col)
        if self._mode == DriverMode.TIMESERIES:
            return
        await self._migrate_removed_columns_upsert(schema, columns)

    async def drop_orphan_tables(self, known_tables: set[str]) -> None:
        """Drop tables in the DB that are not in *known_tables* (the current HQ schema)."""
        if self._mode == DriverMode.TIMESERIES:
            return
        await self._drop_orphan_tables_upsert(known_tables)

    async def _migrate_new_table_upsert(self, schema: TableSchema) -> None:
        raise NotImplementedError

    async def _migrate_new_columns_upsert(self, schema: TableSchema, columns: list[str]) -> None:
        raise NotImplementedError

    async def _migrate_changed_columns_upsert(self, schema: TableSchema, columns: list[str]) -> None:
        pass  # default: no-op (override in dialects that support ALTER COLUMN)

    async def _migrate_removed_columns_upsert(self, schema: TableSchema, columns: list[str]) -> None:
        pass  # default: no-op (override in dialects that support DROP COLUMN)

    async def _drop_orphan_tables_upsert(self, known_tables: set[str]) -> None:
        pass  # default: no-op (override in SQL dialects)

    # ── Time-series: dialect hook methods ─────────────────────────────────────
    # Defaults match PostgreSQL / generic SQL.  Override per dialect as needed.

    def _ensure_events_schema_sql(self, schema_name: str) -> str:
        """SQL to ensure the events schema exists. Return '' for dialects with no schema concept."""
        return f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'

    def _qualify_events_table(self, table: str) -> str:
        """Fully-qualified events table name."""
        return f'"{self._events_schema}"."{table}_events"'

    def _qualify_events_view(self, view_name: str) -> str:
        """Fully-qualified events view name."""
        return f'"{self._events_schema}"."{view_name}"'

    def _quote_id(self, name: str) -> str:
        """Quote a column/table identifier. Override per dialect."""
        return f'"{name}"'

    def _json_extract(self, column: str, field: str) -> str:
        """Dialect-specific JSON field extraction as text. Default: PostgreSQL ->> operator."""
        return f"{column}->>'{field}'"

    def _auto_increment_pk_def(self) -> str:
        """Column definition for the auto-increment _seq primary key."""
        return "BIGSERIAL PRIMARY KEY"

    def _json_column_type(self) -> str:
        """SQL type for JSON blob columns."""
        return "JSONB"

    def _build_create_or_replace_view_sql(self, qualified_view_name: str, select_sql: str) -> str:
        """CREATE OR REPLACE VIEW statement. Override for dialects with different syntax."""
        return f"CREATE OR REPLACE VIEW {qualified_view_name} AS\n{select_sql}"

    def _build_ensure_events_table_sql(self, table: str) -> str:
        """DDL to ensure the events table exists. Override for dialects lacking IF NOT EXISTS."""
        qt = self._qualify_events_table(table)
        jt = self._json_column_type()
        pk = self._auto_increment_pk_def()
        return (
            f"CREATE TABLE IF NOT EXISTS {qt} (\n"
            f"  _seq         {pk},\n"
            f"  _event_id    TEXT,\n"
            f"  _operation   TEXT NOT NULL,\n"
            f"  _entity_id   TEXT,\n"
            f"  _timestamp   BIGINT,\n"
            f"  _mvcc_ts     TEXT,\n"
            f"  _company_id  TEXT,\n"
            f"  _location_id TEXT,\n"
            f"  _model_ver   TEXT,\n"
            f"  _diff        TEXT,\n"
            f"  _before      {jt},\n"
            f"  _after       {jt}\n"
            f")"
        )

    # ── Time-series: DDL builders ─────────────────────────────────────────────

    def _build_current_view_sql(self, table: str) -> str:
        qt = self._qualify_events_table(table)
        qv = self._qualify_events_view(f"current_{table}")
        select_sql = (
            f"SELECT * FROM (\n"
            f"  SELECT *,\n"
            f"    ROW_NUMBER() OVER (PARTITION BY _entity_id ORDER BY _timestamp DESC, _seq DESC) AS rn\n"
            f"  FROM {qt}\n"
            f") ranked\n"
            f"WHERE rn = 1 AND _operation <> 'DELETE'"
        )
        return self._build_create_or_replace_view_sql(qv, select_sql)

    def _build_history_view_sql(self, table: str, schema: TableSchema) -> str:
        qt = self._qualify_events_table(table)
        qv = self._qualify_events_view(f"{table}_history")
        projections = ",\n".join(
            f"  COALESCE({self._json_extract('_after', col.name)}, "
            f"{self._json_extract('_before', col.name)}) AS {self._quote_id(col.name)}"
            for col in schema.column_defs()
        )
        select_sql = (
            "SELECT\n"
            "  _seq, _event_id, _operation, _entity_id, _timestamp, _mvcc_ts,\n"
            "  _company_id, _location_id, _model_ver"
            + (f",\n{projections}" if projections else "")
            + f"\nFROM {qt}"
        )
        return self._build_create_or_replace_view_sql(qv, select_sql)

    def _build_unified_view_sql(self, table: str, schema: TableSchema) -> str:
        """
        Builds a unified view that merges the CDC event stream with the standard mirror table.
        Records that have CDC events use the event-derived state (authoritative).
        Records that exist only in the mirror table (e.g. pre-import backfill not yet touched
        by CDC) are included from the mirror table directly so the full dataset is visible.
        The view is named ``{table}_unified`` in the events schema.
        Returns an empty string when the schema has no columns.
        """
        cols = [c.name for c in schema.column_defs()]
        if not cols:
            return ""

        qt = self._qualify_events_table(table)
        mirror_table = self._quote_id(table)
        qv = self._qualify_events_view(f"{table}_unified")

        # _entity_id stores Key[-1] — the single entity identifier string (last CRDB key
        # segment). For all Shopmonkey tables this is the "id" column.
        pk_cols = [c.name for c in schema.column_defs() if c.primary_key]
        entity_id_col = pk_cols[0] if pk_cols else "id"

        # Events side: project each data column out of the JSON payload.
        event_projections = ",\n".join(
            f"  COALESCE({self._json_extract('_after', col)}, "
            f"{self._json_extract('_before', col)}) AS {self._quote_id(col)}"
            for col in cols
        )
        # Mirror-table side: select columns directly (aliased to avoid ambiguity).
        mirror_projections = ",\n".join(
            f"  s.{self._quote_id(col)}"
            for col in cols
        )

        select_sql = (
            f"SELECT\n{event_projections}\n"
            f"FROM (\n"
            f"  SELECT *,\n"
            f"    ROW_NUMBER() OVER (PARTITION BY _entity_id ORDER BY _timestamp DESC, _seq DESC) AS rn\n"
            f"  FROM {qt}\n"
            f") ranked\n"
            f"WHERE rn = 1 AND _operation <> 'DELETE'\n"
            f"\nUNION ALL\n\n"
            f"SELECT\n{mirror_projections}\n"
            f"FROM {mirror_table} s\n"
            f"WHERE NOT EXISTS (\n"
            f"  SELECT 1 FROM {qt} e\n"
            f"  WHERE e._entity_id = s.{self._quote_id(entity_id_col)}\n"
            f")"
        )
        return self._build_create_or_replace_view_sql(qv, select_sql)

    async def _try_create_unified_view(self, table: str, schema: TableSchema) -> None:
        """Attempt to create/replace the unified view; silently skip if the mirror table
        does not yet exist (e.g. server has only run in time-series mode with no prior import)."""
        unified_sql = self._build_unified_view_sql(table, schema)
        if not unified_sql:
            return
        try:
            await self._execute_ddl(unified_sql)
        except Exception as exc:
            self._log.debug(
                "[%s] Skipping %s_unified view (mirror table may not exist yet): %s",
                self.__class__.__name__, table, exc,
            )

    async def _ensure_events_table_and_views(self, table: str, schema: TableSchema) -> None:
        """Ensure the events schema, table, and all three views exist for a source table."""
        schema_sql = self._ensure_events_schema_sql(self._events_schema)
        if schema_sql:
            await self._execute_ddl(schema_sql)
        await self._execute_ddl(self._build_ensure_events_table_sql(table))
        await self._execute_ddl(self._build_current_view_sql(table))
        await self._execute_ddl(self._build_history_view_sql(table, schema))
        # The unified view joins against the standard mirror table.  Only attempt creation
        # when that table might already exist (populated by a prior bulk import).  If the
        # mirror table is absent the DB raises an error which we catch and log at DEBUG.
        await self._try_create_unified_view(table, schema)

    # ── Time-series: shared INSERT param builder ───────────────────────────────

    def _build_insert_event_params(self, evt: DbChangeEvent) -> tuple[str, list[Any]]:
        """
        Returns (qualified_table, params_list) for a time-series INSERT.
        Column order: _event_id, _operation, _entity_id, _timestamp, _mvcc_ts,
                      _company_id, _location_id, _model_ver, _diff, _before, _after
        """
        qt = self._qualify_events_table(evt.table)
        diff_json = json.dumps(evt.diff) if evt.diff else None
        before_str = self._normalize_json(evt.before)
        after_str = self._normalize_json(evt.after)
        params: list[Any] = [
            evt.id,
            evt.operation,
            evt.get_primary_key(),
            evt.timestamp,
            evt.mvcc_timestamp or None,
            evt.company_id or None,
            evt.location_id or None,
            evt.model_version or None,
            diff_json,
            before_str,
            after_str,
        ]
        return qt, params

    # ── SQL helpers ────────────────────────────────────────────────────────────

    def _log_sql_error(self, sql: str, params: Any = None) -> None:
        """Log a failing SQL statement at two verbosity levels.

        Logs a truncated version at ERROR (visible on the console in normal mode)
        and the full SQL plus params at DEBUG (captured by the file sink only,
        since the file sink uses Debug minimum level while the console uses
        Information in normal mode).
        """
        short_sql = sql[:500] + "…" if len(sql) > 500 else sql
        self._log.error("[%s] SQL execution failed. SQL: %s", self.__class__.__name__, short_sql)
        params_repr = repr(params) if params is not None else ""
        self._log.debug(
            "[%s] Full SQL: %s%s",
            self.__class__.__name__,
            sql,
            f" | params={params_repr}" if params_repr else "",
        )

    def _normalize_json(self, raw: bytes | None) -> str | None:
        """Re-serialize raw JSON bytes through the standard parser so that all
        control characters and non-ASCII codepoints are properly escaped before
        insertion into a JSON-typed column (MySQL/PostgreSQL).

        Raw payloads sourced from CDC may contain unescaped control characters
        inside JSON string values that are valid per msgpack but rejected by
        strict JSON column validators (MySQL: 'Invalid encoding in string').
        Re-serializing via json.loads + json.dumps normalizes them to \\uXXXX.
        """
        if not raw:
            return None
        try:
            return json.dumps(json.loads(raw))
        except (json.JSONDecodeError, ValueError, UnicodeDecodeError):
            # Invalid UTF-8 bytes — replace them and try again.
            try:
                sanitized = raw.decode('utf-8', errors='replace')
                return json.dumps(json.loads(sanitized))
            except Exception:
                self._log.warning(
                    "Dropping unrecoverable JSON payload (%d bytes) — storing NULL", len(raw)
                )
                return None

    @staticmethod
    def _flatten_object(event: DbChangeEvent) -> dict[str, Any]:
        """Return the decoded payload as a flat {column: value} dict."""
        obj = event.get_object()
        return obj or {}

    @staticmethod
    def _coerce_value(v: Any) -> Any:
        """Ensure values are safe to bind into TEXT/NVARCHAR upsert columns.

        - dicts and lists are JSON-serialised.
        - booleans are coerced to "true"/"false" (checked before int since
          bool is a subclass of int in Python).
        - ints and floats are coerced to their string representation.
        - None is left as None (NULL).
        - strings pass through unchanged.
        """
        if v is None:
            return None
        if isinstance(v, (dict, list)):
            return json.dumps(v)
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, (int, float)):
            return str(v)
        return v
