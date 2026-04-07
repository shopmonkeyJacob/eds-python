"""Azure Event Hubs driver — publishes CDC events as JSON messages."""

from __future__ import annotations

import json
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

        batch = await self._producer.create_batch()
        for evt in self._pending:
            body = json.dumps({
                "operation": evt.operation,
                "id": evt.id,
                "table": evt.table,
                "timestamp": evt.timestamp,
                "data": evt.get_object(),
            }).encode()
            batch.add(EventData(body))

        await self._producer.send_batch(batch)
        self._pending = []

    async def test(self, url: str) -> None:
        import logging
        await self.start(DriverConfig(url=url, logger=logging.getLogger(__name__), data_dir=""))
        assert self._producer
        async with self._producer:
            pass  # successful connect is enough
