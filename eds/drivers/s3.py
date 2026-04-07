"""Amazon S3 driver — uploads NDJSON events as JSON objects.

For bulk import, raw .ndjson.gz files are uploaded directly.
For CDC streaming, each event is stored as {prefix}/{table}/{timestamp}-{id}.json.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any
from urllib.parse import urlparse, parse_qs

import boto3  # type: ignore[import]

from eds.core.driver import (
    DriverField, FieldError,
    required_string, optional_string, optional_password, get_str,
)
from eds.core.driver import Driver, DriverConfig
from eds.core.models import DbChangeEvent


class S3Driver(Driver):

    def __init__(self) -> None:
        self._client: Any = None
        self._bucket: str = ""
        self._prefix: str = ""
        self._loop: asyncio.AbstractEventLoop | None = None
        self._pending: list[DbChangeEvent] = []

    def name(self) -> str:
        return "Amazon S3"

    def description(self) -> str:
        return "Stream CDC events to Amazon S3 (or compatible) as JSON objects."

    def example_url(self) -> str:
        return "s3://my-bucket/optional-prefix?region=us-east-1"

    def configuration(self) -> list[DriverField]:
        return [
            required_string("Bucket", "S3 bucket name"),
            optional_string("Prefix", "Key prefix (folder)"),
            optional_string("Region", "AWS region", "us-east-1"),
            optional_string("AccessKey", "AWS access key ID (uses credential chain if omitted)"),
            optional_password("SecretKey", "AWS secret access key"),
        ]

    def validate(self, values: dict[str, Any]) -> tuple[str, list[FieldError]]:
        bucket = get_str(values, "Bucket")
        if not bucket:
            return "", [FieldError("Bucket", "Bucket is required")]
        prefix = get_str(values, "Prefix")
        region = get_str(values, "Region", "us-east-1")
        key = get_str(values, "AccessKey")
        secret = get_str(values, "SecretKey")
        auth = f"{key}:{secret}@" if key else ""
        path = f"/{prefix}" if prefix else ""
        return f"s3://{auth}{bucket}{path}?region={region}", []

    def max_batch_size(self) -> int:
        return 500

    async def start(self, config: DriverConfig) -> None:
        self._loop = asyncio.get_event_loop()
        u = urlparse(config.url)
        qs = parse_qs(u.query)
        self._bucket = u.hostname or ""
        self._prefix = u.path.lstrip("/")
        region = (qs.get("region") or ["us-east-1"])[0]
        kwargs: dict[str, Any] = {"region_name": region}
        if u.username:
            kwargs["aws_access_key_id"] = u.username
            kwargs["aws_secret_access_key"] = u.password or ""
        self._client = boto3.client("s3", **kwargs)

    async def stop(self) -> None:
        await self.flush()

    async def process(self, event: DbChangeEvent) -> bool:
        self._pending.append(event)
        return False

    async def flush(self) -> None:
        if not self._pending or not self._client:
            return

        def _upload(events: list[DbChangeEvent]) -> None:
            for evt in events:
                key_parts = [self._prefix, evt.table, f"{evt.timestamp}-{evt.id}.json"]
                key = "/".join(p for p in key_parts if p)
                body = json.dumps({
                    "operation": evt.operation,
                    "id": evt.id,
                    "table": evt.table,
                    "timestamp": evt.timestamp,
                    "data": evt.get_object(),
                }).encode()
                self._client.put_object(Bucket=self._bucket, Key=key, Body=body,
                                        ContentType="application/json")

        events = list(self._pending)
        self._pending = []
        assert self._loop
        await self._loop.run_in_executor(None, lambda: _upload(events))

    async def test(self, url: str) -> None:
        await self.start(DriverConfig(url=url, logger=__import__("logging").getLogger(__name__),
                                      data_dir=""))
        assert self._loop
        await self._loop.run_in_executor(None, lambda: self._client.head_bucket(Bucket=self._bucket))
