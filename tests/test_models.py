"""Unit tests for DbChangeEvent."""

import json
import pytest
from eds.core.models import DbChangeEvent


def _make(op: str = "INSERT", table: str = "orders", **kwargs) -> DbChangeEvent:
    base = {"operation": op, "table": table, "id": "abc", "key": ["abc"],
            "modelVersion": "1", "timestamp": 1000, "mvccTimestamp": "1"}
    base.update(kwargs)
    return DbChangeEvent.from_dict(base)


def test_from_dict_insert() -> None:
    after = {"id": "abc", "name": "test"}
    evt = _make(after=after)
    assert evt.operation == "INSERT"
    assert evt.table == "orders"
    assert evt.get_primary_key() == "abc"


def test_get_primary_key_from_key_field() -> None:
    evt = _make(key=["parent", "child-id"])
    assert evt.get_primary_key() == "child-id"


def test_get_primary_key_from_object_fallback() -> None:
    evt = DbChangeEvent.from_dict({
        "operation": "INSERT", "table": "t", "id": "", "key": [],
        "modelVersion": "1", "timestamp": 0, "mvccTimestamp": "",
        "after": {"id": "json-id", "x": 1},
    })
    assert evt.get_primary_key() == "json-id"


def test_get_object_returns_after() -> None:
    after = {"id": "abc", "val": 42}
    evt = _make(after=after)
    obj = evt.get_object()
    assert obj is not None
    assert obj["val"] == 42


def test_get_object_returns_before_for_delete() -> None:
    before = {"id": "abc", "val": 99}
    evt = _make(op="DELETE", before=before)
    obj = evt.get_object()
    assert obj is not None
    assert obj["val"] == 99


def test_omit_properties() -> None:
    after = {"id": "abc", "secret": "hidden", "name": "visible"}
    evt = _make(after=after)
    evt.omit_properties("secret")
    obj = evt.get_object()
    assert obj is not None
    assert "secret" not in obj
    assert obj["name"] == "visible"


def test_str_representation() -> None:
    evt = _make()
    s = str(evt)
    assert "INSERT" in s
    assert "orders" in s


def test_after_as_raw_json_bytes() -> None:
    raw = json.dumps({"id": "x", "v": 1}).encode()
    evt = DbChangeEvent.from_dict({
        "operation": "INSERT", "table": "t", "id": "x", "key": ["x"],
        "modelVersion": "1", "timestamp": 0, "mvccTimestamp": "",
        "after": raw,
    })
    obj = evt.get_object()
    assert obj is not None
    assert obj["v"] == 1
