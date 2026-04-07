"""Kafka driver — publishes CDC events as JSON messages.

Uses confluent-kafka (sync C extension) wrapped in an executor.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from urllib.parse import urlparse

from confluent_kafka import Producer  # type: ignore[import]

from eds.core.driver import (
    DriverField, FieldError,
    required_string, optional_string, get_str,
    Driver, DriverConfig,
)
from eds.core.models import DbChangeEvent


class KafkaDriver(Driver):

    def __init__(self) -> None:
        self._producer: Producer | None = None
        self._topic: str = ""
        self._loop: asyncio.AbstractEventLoop | None = None
        self._pending: list[DbChangeEvent] = []

    def name(self) -> str:
        return "Apache Kafka"

    def description(self) -> str:
        return "Publish CDC events as JSON messages to an Apache Kafka topic."

    def example_url(self) -> str:
        return "kafka://broker-host:9092/topic-name"

    def configuration(self) -> list[DriverField]:
        return [
            required_string("Broker", "Kafka broker address (host:port)"),
            required_string("Topic", "Kafka topic name"),
            optional_string("GroupID", "Consumer group ID", "eds"),
        ]

    def validate(self, values: dict[str, Any]) -> tuple[str, list[FieldError]]:
        errors: list[FieldError] = []
        broker = get_str(values, "Broker")
        topic = get_str(values, "Topic")
        if not broker:
            errors.append(FieldError("Broker", "Broker address is required"))
        if not topic:
            errors.append(FieldError("Topic", "Topic is required"))
        if errors:
            return "", errors
        return f"kafka://{broker}/{topic}", []

    def max_batch_size(self) -> int:
        return 1000

    async def start(self, config: DriverConfig) -> None:
        self._loop = asyncio.get_event_loop()
        u = urlparse(config.url)
        broker = f"{u.hostname}:{u.port or 9092}"
        self._topic = u.path.lstrip("/")
        self._producer = Producer({"bootstrap.servers": broker})

    async def stop(self) -> None:
        await self.flush()
        if self._producer:
            self._producer.flush(timeout=30)

    async def process(self, event: DbChangeEvent) -> bool:
        self._pending.append(event)
        return False

    async def flush(self) -> None:
        if not self._pending or not self._producer:
            return

        def _produce(events: list[DbChangeEvent]) -> None:
            assert self._producer
            for evt in events:
                key = evt.get_primary_key().encode() if evt.get_primary_key() else None
                value = json.dumps({
                    "operation": evt.operation,
                    "id": evt.id,
                    "table": evt.table,
                    "timestamp": evt.timestamp,
                    "data": evt.get_object(),
                }).encode()
                self._producer.produce(
                    self._topic, key=key, value=value,
                    headers={"table": evt.table, "operation": evt.operation},
                )
            self._producer.flush()

        events = list(self._pending)
        self._pending = []
        assert self._loop
        await self._loop.run_in_executor(None, lambda: _produce(events))

    async def test(self, url: str) -> None:
        import logging
        await self.start(DriverConfig(url=url, logger=logging.getLogger(__name__), data_dir=""))
        # A producer doesn't require a test connection; just check config was valid
        await self.stop()
