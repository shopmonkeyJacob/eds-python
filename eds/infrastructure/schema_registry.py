"""Schema registry — mirrors internal/registry and internal/schema.go.

Fetches table schemas from Shopmonkey HQ and caches them locally.
Table version tracking is persisted via the Tracker so DDL migrations
only run when the Shopmonkey data model actually changes.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any

import aiohttp

from eds.core.models import SchemaColumn, TableSchema
from eds.core.tracker import Tracker

_log = logging.getLogger(__name__)

_TABLE_VERSION_KEY_PREFIX = "schema:version:"
_SCHEMA_CACHE_KEY_PREFIX  = "schema:data:"
_SCHEMA_TTL_SECONDS       = 24 * 3600  # 24 hours


@dataclass
class SchemaRegistry:
    """3-tier schema cache: in-memory → SQLite (24 h TTL) → API.

    Warm starts avoid an API round-trip when the schema is already persisted
    from a previous run, matching the behaviour of the .NET SqliteSchemaRegistry.
    """

    api_url: str
    api_key: str
    tracker: Tracker
    _cache: dict[str, dict[str, TableSchema]] = field(default_factory=dict, repr=False)

    async def get_schema(self, table: str, model_version: str) -> TableSchema:
        # Tier 1: in-memory
        cached = self._cache.get(table, {}).get(model_version)
        if cached:
            return cached

        # Tier 2: SQLite (warm-start / cross-session cache)
        sqlite_schema = await self._sqlite_get_schema(table, model_version)
        if sqlite_schema is not None:
            self._cache.setdefault(table, {})[model_version] = sqlite_schema
            return sqlite_schema

        # Tier 3: API
        schema = await self._fetch_schema(table, model_version)
        self._cache.setdefault(table, {})[model_version] = schema
        await self._sqlite_set_schema(table, model_version, schema)
        return schema

    async def get_table_version(self, table: str) -> tuple[bool, str]:
        """Return (found, version) from tracker."""
        val = await self.tracker.get_key(f"{_TABLE_VERSION_KEY_PREFIX}{table}")
        return (True, val) if val else (False, "")

    async def set_table_version(self, table: str, version: str) -> None:
        await self.tracker.set_key(f"{_TABLE_VERSION_KEY_PREFIX}{table}", version)

    # ── SQLite helpers ────────────────────────────────────────────────────────

    async def _sqlite_get_schema(self, table: str, model_version: str) -> TableSchema | None:
        key = f"{_SCHEMA_CACHE_KEY_PREFIX}{table}:{model_version}"
        raw = await self.tracker.get_key(key)
        if not raw:
            return None
        try:
            data = json.loads(raw)
            if time.time() > data["expires_at"]:
                _log.debug(
                    "[schema-registry] SQLite cache expired for %s/%s", table, model_version
                )
                return None
            columns = [
                SchemaColumn(
                    name=col["name"],
                    data_type=col.get("type", "text"),
                    nullable=col.get("nullable", True),
                    primary_key=col.get("primaryKey", False),
                )
                for col in data["columns"]
            ]
            schema = TableSchema(table=table, model_version=model_version)
            schema._columns = columns
            _log.debug(
                "[schema-registry] SQLite cache hit for %s/%s", table, model_version
            )
            return schema
        except Exception as exc:
            _log.debug(
                "[schema-registry] SQLite cache read failed for %s/%s (%s) — falling back to API",
                table, model_version, exc,
            )
            return None

    async def _sqlite_set_schema(
        self, table: str, model_version: str, schema: TableSchema
    ) -> None:
        key = f"{_SCHEMA_CACHE_KEY_PREFIX}{table}:{model_version}"
        payload = {
            "expires_at": time.time() + _SCHEMA_TTL_SECONDS,
            "columns": [
                {
                    "name": col.name,
                    "type": col.data_type,
                    "nullable": col.nullable,
                    "primaryKey": col.primary_key,
                }
                for col in schema.column_defs()
            ],
        }
        try:
            await self.tracker.set_key(key, json.dumps(payload))
        except Exception as exc:
            _log.debug("[schema-registry] SQLite cache write failed: %s", exc)

    # ── API fetch ─────────────────────────────────────────────────────────────

    async def _fetch_schema(self, table: str, model_version: str) -> TableSchema:
        url = f"{self.api_url}/v3/schema/{table}/{model_version}"
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers={"Authorization": f"Bearer {self.api_key}"},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                resp.raise_for_status()
                data: dict[str, Any] = await resp.json()

        columns = [
            SchemaColumn(
                name=col["name"],
                data_type=col.get("type", "text"),
                nullable=col.get("nullable", True),
                primary_key=col.get("primaryKey", False),
            )
            for col in data.get("columns", [])
        ]
        schema = TableSchema(table=table, model_version=model_version)
        schema._columns = columns
        return schema
