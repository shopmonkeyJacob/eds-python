"""Driver abstractions — mirrors internal/driver.go."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

if TYPE_CHECKING:
    from eds.infrastructure.tracker import SqliteTracker
    from eds.infrastructure.schema_registry import SchemaRegistry

from eds.core.models import DbChangeEvent, TableSchema


class DriverMode(str, Enum):
    UPSERT = "upsert"
    TIMESERIES = "timeseries"


# Raised by a driver to signal it has been stopped
class DriverStoppedError(Exception):
    pass


@dataclass
class DriverField:
    name: str
    type: str  # "string" | "number" | "boolean"
    description: str
    required: bool = True
    default: str | None = None
    format: str | None = None  # "password"


@dataclass
class FieldError:
    field: str
    message: str

    def __str__(self) -> str:
        return self.message


@dataclass
class DriverConfig:
    url: str
    logger: logging.Logger
    data_dir: str
    tracker: SqliteTracker | None = None
    schema_registry: SchemaRegistry | None = None
    mode: DriverMode = DriverMode.UPSERT
    events_schema: str = "eds_events"


class Driver(ABC):
    """Base interface all EDS drivers must implement."""

    @abstractmethod
    async def start(self, config: DriverConfig) -> None:
        """Called once at startup."""

    @abstractmethod
    async def stop(self) -> None:
        """Called once at shutdown."""

    @abstractmethod
    def max_batch_size(self) -> int:
        """Max events before Flush is forced. Return -1 for no limit."""

    @abstractmethod
    async def process(self, event: DbChangeEvent) -> bool:
        """Process one event. Return True to force an immediate flush."""

    @abstractmethod
    async def flush(self) -> None:
        """Commit all pending events."""

    @abstractmethod
    async def test(self, url: str) -> None:
        """Test connectivity. Raise on failure."""

    @abstractmethod
    def configuration(self) -> list[DriverField]:
        """Return the list of configuration fields shown in the HQ UI."""

    @abstractmethod
    def validate(self, values: dict[str, Any]) -> tuple[str, list[FieldError]]:
        """Validate config values and return (url, errors)."""

    # Optional helpers -------------------------------------------------------

    def name(self) -> str:
        return self.__class__.__name__

    def description(self) -> str:
        return ""

    def example_url(self) -> str:
        return ""

    def help_text(self) -> str:
        return ""

    def aliases(self) -> list[str]:
        return []

    def supports_migration(self) -> bool:
        return False

    async def migrate_new_table(self, schema: TableSchema) -> None:  # noqa: ARG002
        raise NotImplementedError

    async def migrate_new_columns(self, schema: TableSchema, columns: list[str]) -> None:  # noqa: ARG002
        raise NotImplementedError

    async def migrate_changed_columns(self, schema: TableSchema, columns: list[str]) -> None:  # noqa: ARG002
        raise NotImplementedError

    async def migrate_removed_columns(self, schema: TableSchema, columns: list[str]) -> None:  # noqa: ARG002
        raise NotImplementedError

    async def drop_orphan_tables(self, known_tables: set[str]) -> None:  # noqa: ARG002
        raise NotImplementedError

    def supports_direct_import(self) -> bool:
        """Return True if this driver can receive raw .ndjson.gz files directly."""
        return False

    async def direct_import(self, file_table_pairs: list[tuple[str, Path]]) -> None:
        """Import raw .ndjson.gz files without row-by-row parsing.
        Only called when supports_direct_import() returns True.
        file_table_pairs: list of (table_name, local_file_path) tuples.
        """
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Driver registry
# ---------------------------------------------------------------------------

_registry: dict[str, Driver] = {}
_alias_registry: dict[str, str] = {}  # alias scheme → canonical scheme


def register_driver(scheme: str, driver: Driver) -> None:
    _registry[scheme] = driver
    for alias in driver.aliases():
        _alias_registry[alias] = scheme


def _resolve(scheme: str) -> Driver | None:
    d = _registry.get(scheme)
    if d:
        return d
    canonical = _alias_registry.get(scheme)
    return _registry.get(canonical) if canonical else None


async def new_driver(url: str, config: DriverConfig) -> Driver:
    scheme = urlparse(url).scheme
    driver = _resolve(scheme)
    if driver is None:
        raise ValueError(f"No driver registered for scheme '{scheme}'")
    await driver.start(config)
    return driver


def get_driver_metadata() -> list[dict[str, Any]]:
    result = []
    for scheme, driver in _registry.items():
        result.append(
            {
                "scheme": scheme,
                "name": driver.name(),
                "description": driver.description(),
                "example_url": driver.example_url(),
                "help": driver.help_text(),
                "supports_migration": driver.supports_migration(),
            }
        )
    return result


# ---------------------------------------------------------------------------
# Field helpers (mirrors Go helper functions)
# ---------------------------------------------------------------------------

def required_string(name: str, description: str, default: str | None = None) -> DriverField:
    return DriverField(name=name, type="string", description=description, required=True, default=default)


def optional_string(name: str, description: str, default: str | None = None) -> DriverField:
    return DriverField(name=name, type="string", description=description, required=False, default=default)


def optional_password(name: str, description: str) -> DriverField:
    return DriverField(name=name, type="string", format="password", description=description, required=False)


def optional_number(name: str, description: str, default: int | None = None) -> DriverField:
    return DriverField(
        name=name, type="number", description=description, required=False,
        default=str(default) if default is not None else None,
    )


def get_str(values: dict[str, Any], name: str, default: str = "") -> str:
    v = values.get(name, default)
    return str(v) if v is not None else default


def get_int(values: dict[str, Any], name: str, default: int = 0) -> int:
    v = values.get(name)
    if v is None:
        return default
    try:
        return int(v)
    except (TypeError, ValueError):
        return default
