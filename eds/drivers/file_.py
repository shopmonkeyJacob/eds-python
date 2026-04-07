"""Local file driver — writes CDC events as NDJSON to a directory.

Mirrors EDS.Drivers.File — primarily useful for testing and local export.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import aiofiles  # type: ignore[import]

from eds.core.driver import (
    DriverField, FieldError,
    required_string, get_str,
    Driver, DriverConfig,
)
from eds.core.models import DbChangeEvent


class FileDriver(Driver):

    def __init__(self) -> None:
        self._dir: str = ""
        self._pending: list[DbChangeEvent] = []

    def name(self) -> str:
        return "Local File"

    def description(self) -> str:
        return "Write CDC events as NDJSON to a local directory (testing / export)."

    def example_url(self) -> str:
        return "file:///path/to/output"

    def configuration(self) -> list[DriverField]:
        return [required_string("Path", "Absolute path to output directory")]

    def validate(self, values: dict[str, Any]) -> tuple[str, list[FieldError]]:
        path = get_str(values, "Path")
        if not path:
            return "", [FieldError("Path", "Path is required")]
        return f"file://{path}", []

    def max_batch_size(self) -> int:
        return -1

    async def start(self, config: DriverConfig) -> None:
        u = urlparse(config.url)
        self._dir = u.path or u.netloc
        os.makedirs(self._dir, exist_ok=True)

    async def stop(self) -> None:
        await self.flush()

    async def process(self, event: DbChangeEvent) -> bool:
        self._pending.append(event)
        return False

    async def flush(self) -> None:
        if not self._pending:
            return
        # Group by table
        by_table: dict[str, list[DbChangeEvent]] = {}
        for evt in self._pending:
            by_table.setdefault(evt.table, []).append(evt)

        for table, events in by_table.items():
            table_dir = Path(self._dir) / table
            table_dir.mkdir(parents=True, exist_ok=True)
            # Append to a single rolling NDJSON file per table session
            out_path = table_dir / "events.ndjson"
            async with aiofiles.open(out_path, "a", encoding="utf-8") as f:
                for evt in events:
                    line = json.dumps({
                        "operation": evt.operation,
                        "id": evt.id,
                        "table": evt.table,
                        "timestamp": evt.timestamp,
                        "data": evt.get_object(),
                    })
                    await f.write(line + "\n")

        self._pending = []

    async def test(self, url: str) -> None:
        import logging
        await self.start(DriverConfig(url=url, logger=logging.getLogger(__name__), data_dir=""))
        path = Path(self._dir)
        if not path.exists():
            raise FileNotFoundError(f"Directory does not exist: {self._dir}")
