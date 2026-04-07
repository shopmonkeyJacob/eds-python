"""Core data models — mirrors internal/dbchange.go and internal/schema.go."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


@dataclass
class DbChangeEvent:
    """A single CDC record delivered via NATS JetStream."""

    operation: str = ""           # INSERT | UPDATE | DELETE
    id: str = ""
    table: str = ""
    key: list[str] = field(default_factory=list)
    model_version: str = ""       # modelVersion in JSON
    company_id: str | None = None
    location_id: str | None = None
    user_id: str | None = None
    before: bytes | None = None   # raw JSON
    after: bytes | None = None    # raw JSON
    diff: list[str] = field(default_factory=list)
    timestamp: int = 0            # Unix milliseconds
    mvcc_timestamp: str = ""

    # Not serialised — set by the consumer after decode
    imported: bool = False

    # Cached decoded object (after or before payload)
    _object: dict[str, Any] | None = field(default=None, repr=False, compare=False)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "DbChangeEvent":
        evt = cls(
            operation=d.get("operation", ""),
            id=d.get("id", ""),
            table=d.get("table", ""),
            key=d.get("key") or [],
            model_version=d.get("modelVersion", ""),
            company_id=d.get("companyId"),
            location_id=d.get("locationId"),
            user_id=d.get("userId"),
            diff=d.get("diff") or [],
            timestamp=d.get("timestamp", 0),
            mvcc_timestamp=d.get("mvccTimestamp", ""),
            imported=d.get("imported", False),
        )
        before_raw = d.get("before")
        after_raw = d.get("after")
        if isinstance(before_raw, (dict, list)):
            evt.before = json.dumps(before_raw).encode()
        elif isinstance(before_raw, (bytes, str)):
            evt.before = before_raw if isinstance(before_raw, bytes) else before_raw.encode()
        if isinstance(after_raw, (dict, list)):
            evt.after = json.dumps(after_raw).encode()
        elif isinstance(after_raw, (bytes, str)):
            evt.after = after_raw if isinstance(after_raw, bytes) else after_raw.encode()
        return evt

    def get_primary_key(self) -> str:
        if self.key:
            return self.key[-1]
        obj = self.get_object()
        if obj and isinstance(obj.get("id"), str):
            return obj["id"]
        return ""

    def get_object(self) -> dict[str, Any] | None:
        if self._object is not None:
            return self._object
        raw = self.after if self.after else self.before
        if raw:
            self._object = json.loads(raw)
            return self._object
        return None

    def omit_properties(self, *props: str) -> None:
        obj = self.get_object()
        if obj is None:
            return
        for p in props:
            obj.pop(p, None)
        self._object = obj
        # Reserialise so after/before stay in sync
        payload = json.dumps(obj).encode()
        if self.after:
            self.after = payload
        elif self.before:
            self.before = payload

    def __str__(self) -> str:
        return (
            f"DbChangeEvent[op={self.operation},table={self.table},"
            f"id={self.id},pk={self.get_primary_key()}]"
        )


@dataclass
class SchemaColumn:
    name: str
    data_type: str
    nullable: bool = True
    primary_key: bool = False


@dataclass
class TableSchema:
    table: str
    model_version: str
    _columns: list[SchemaColumn] = field(default_factory=list)

    def columns(self) -> list[str]:
        return [c.name for c in self._columns]

    def column_defs(self) -> list[SchemaColumn]:
        return self._columns
