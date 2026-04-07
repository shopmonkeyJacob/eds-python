"""Schema registry — mirrors internal/registry and internal/schema.go.

Fetches table schemas from Shopmonkey HQ and caches them locally.
Table version tracking is persisted via the Tracker so DDL migrations
only run when the Shopmonkey data model actually changes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import aiohttp

from eds.core.models import SchemaColumn, TableSchema
from eds.core.tracker import Tracker

_log = logging.getLogger(__name__)

_TABLE_VERSION_KEY_PREFIX = "schema:version:"


@dataclass
class SchemaRegistry:
    api_url: str
    api_key: str
    tracker: Tracker
    _cache: dict[str, dict[str, TableSchema]] = field(default_factory=dict, repr=False)

    async def get_schema(self, table: str, model_version: str) -> TableSchema:
        cached = self._cache.get(table, {}).get(model_version)
        if cached:
            return cached
        schema = await self._fetch_schema(table, model_version)
        self._cache.setdefault(table, {})[model_version] = schema
        return schema

    async def get_table_version(self, table: str) -> tuple[bool, str]:
        """Return (found, version) from tracker."""
        val = await self.tracker.get_key(f"{_TABLE_VERSION_KEY_PREFIX}{table}")
        return (True, val) if val else (False, "")

    async def set_table_version(self, table: str, version: str) -> None:
        await self.tracker.set_key(f"{_TABLE_VERSION_KEY_PREFIX}{table}", version)

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
