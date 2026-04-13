"""Azure Event Hubs driver — publishes CDC events as JSON messages."""

from __future__ import annotations

import gzip
import json
import logging
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, parse_qs, unquote

from azure.eventhub.aio import EventHubProducerClient  # type: ignore[import]
from azure.eventhub import EventData  # type: ignore[import]

from eds.core.driver import (
    DriverField, FieldError,
    required_string, optional_password, get_str,
    Driver, DriverConfig,
)
from eds.core.models import DbChangeEvent


class EventHubDriver(Driver):

    def __init__(self) -> None:
        self._producer: EventHubProducerClient | None = None
        self._hub_name: str = ""
        self._pending: list[DbChangeEvent] = []

    def name(self) -> str:
        return "Azure Event Hubs"

    def description(self) -> str:
        return "Publish CDC events as JSON messages to Azure Event Hubs."

    def example_url(self) -> str:
        return "eventhub://namespace.servicebus.windows.net/hub-name?connection-string=..."

    def configuration(self) -> list[DriverField]:
        return [
            required_string("Namespace", "Event Hubs namespace (FQDN)"),
            required_string("HubName", "Event Hub name"),
            optional_password("ConnectionString", "Connection string (shared access)"),
        ]

    def validate(self, values: dict[str, Any]) -> tuple[str, list[FieldError]]:
        errors: list[FieldError] = []
        ns = get_str(values, "Namespace")
        hub = get_str(values, "HubName")
        if not ns:
            errors.append(FieldError("Namespace", "Namespace is required"))
        if not hub:
            errors.append(FieldError("HubName", "Hub name is required"))
        if errors:
            return "", errors
        conn = get_str(values, "ConnectionString")
        qs = f"?connection-string={conn}" if conn else ""
        return f"eventhub://{ns}/{hub}{qs}", []

    def max_batch_size(self) -> int:
        return 500

    async def start(self, config: DriverConfig) -> None:
        u = urlparse(config.url)
        qs = parse_qs(u.query)
        self._hub_name = u.path.lstrip("/")
        conn_str = unquote((qs.get("connection-string") or [""])[0])
        if conn_str:
            self._producer = EventHubProducerClient.from_connection_string(
                conn_str, eventhub_name=self._hub_name
            )
        else:
            from azure.identity.aio import DefaultAzureCredential
            fqdn = u.hostname or ""
            self._producer = EventHubProducerClient(
                fully_qualified_namespace=fqdn,
                eventhub_name=self._hub_name,
                credential=DefaultAzureCredential(),
            )

    async def stop(self) -> None:
        await self.flush()
        if self._producer:
            await self._producer.close()

    async def process(self, event: DbChangeEvent) -> bool:
        self._pending.append(event)
        return False

    async def flush(self) -> None:
        if not self._pending or not self._producer:
            return

        batches: list = []
        batch = await self._producer.create_batch()
        for evt in self._pending:
            body = json.dumps({
                "operation": evt.operation,
                "id": evt.id,
                "table": evt.table,
                "timestamp": evt.timestamp,
                "data": evt.get_object(),
            }).encode()
            try:
                batch.add(EventData(body))
            except Exception:
                # Batch is full — queue it and open a fresh one
                batches.append(batch)
                batch = await self._producer.create_batch()
                batch.add(EventData(body))
        batches.append(batch)

        for b in batches:
            await self._producer.send_batch(b)
        self._pending = []

    async def test(self, url: str) -> None:
        await self.start(DriverConfig(url=url, logger=logging.getLogger(__name__), data_dir=""))
        assert self._producer
        async with self._producer:
            pass  # successful connect is enough

    # ── Direct import ──────────────────────────────────────────────────────────

    def supports_direct_import(self) -> bool:
        return True

    async def direct_import(self, file_table_pairs: list[tuple[str, Path]]) -> None:
        """Parse .ndjson.gz files and publish each record as an EventHub message."""
        log = logging.getLogger(__name__)
        batch_size = self.max_batch_size()
        total = 0
        for table, path in file_table_pairs:
            log.info("[import] Publishing %s", path.name)
            count = 0
            opener = gzip.open if path.suffix == ".gz" else open
            with opener(path, "rb") as fh:  # type: ignore[call-overload]
                for raw_line in fh:
                    raw_line = raw_line.strip()
                    if not raw_line:
                        continue
                    try:
                        row = json.loads(raw_line)
                    except json.JSONDecodeError as exc:
                        log.warning("[import] Skipping invalid JSON line in %s: %s", path.name, exc)
                        continue
                    evt = _build_import_event(row, table, raw_line)
                    await self.process(evt)
                    count += 1
                    if count % batch_size == 0:
                        await self.flush()
            if count % batch_size != 0:
                await self.flush()
            log.info("[import] %s: %d record(s) published", path.name, count)
            total += count
        log.info("[import] Published %d total record(s) to EventHub '%s'", total, self._hub_name)


def _build_import_event(row: dict, table: str, raw_line: bytes) -> DbChangeEvent:
    """Build a synthetic INSERT DbChangeEvent from a raw CRDB export row."""
    record_id = row.get("id") or str(uuid.uuid4())
    company_id = row.get("companyId")
    location_id = row.get("locationId")
    # Mirrors Go: LocationId uses locationId but falls back to companyId when locationId is absent.
    effective_location_id = company_id or location_id
    return DbChangeEvent(
        operation="INSERT",
        id=record_id,
        table=table,
        key=[record_id],
        company_id=company_id,
        location_id=effective_location_id,
        after=raw_line,
        imported=True,
    )
