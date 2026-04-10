"""Test helpers for driver integration tests — mirrors EDS.Integration.Tests.Helpers."""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from eds.core.driver import DriverConfig
from eds.core.models import DbChangeEvent, SchemaColumn, TableSchema


def orders_schema(table: str = "eds_test_orders") -> TableSchema:
    """Standard mixed-column schema covering all column kinds."""
    schema = TableSchema(table=table, model_version="1")
    schema._columns = [
        SchemaColumn(name="id",        data_type="text", nullable=False, primary_key=True),
        SchemaColumn(name="name",      data_type="text", nullable=True),
        SchemaColumn(name="amount",    data_type="text", nullable=True),
        SchemaColumn(name="qty",       data_type="text", nullable=True),
        SchemaColumn(name="active",    data_type="text", nullable=True),
        SchemaColumn(name="createdAt", data_type="text", nullable=True),
        SchemaColumn(name="meta",      data_type="text", nullable=True),
        SchemaColumn(name="tags",      data_type="text", nullable=True),
    ]
    return schema


def make_config(url: str) -> DriverConfig:
    return DriverConfig(url=url, logger=logging.getLogger("test"), data_dir="")


def make_insert(table: str, row_id: str, payload: dict[str, Any]) -> DbChangeEvent:
    return DbChangeEvent(
        operation="INSERT",
        id=str(uuid.uuid4()),
        table=table,
        key=[row_id],
        model_version="1",
        after=json.dumps(payload).encode(),
    )


def make_update(
    table: str,
    row_id: str,
    payload: dict[str, Any],
    diff: list[str] | None = None,
) -> DbChangeEvent:
    return DbChangeEvent(
        operation="UPDATE",
        id=str(uuid.uuid4()),
        table=table,
        key=[row_id],
        model_version="1",
        after=json.dumps(payload).encode(),
        diff=diff or [],
    )


def make_delete(table: str, row_id: str) -> DbChangeEvent:
    # key[-1] is the primary key value, matching the CDC wire format.
    return DbChangeEvent(
        operation="DELETE",
        id=str(uuid.uuid4()),
        table=table,
        key=["", row_id],
        model_version="1",
    )
