"""Azure Blob Storage driver — mirrors EDS.Drivers.AzureBlob."""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlparse, parse_qs, unquote

from azure.storage.blob.aio import BlobServiceClient  # type: ignore[import]

from eds.core.driver import (
    DriverField, FieldError,
    required_string, optional_string, optional_password, get_str,
    Driver, DriverConfig,
)
from eds.core.models import DbChangeEvent


class AzureBlobDriver(Driver):

    def __init__(self) -> None:
        self._client: BlobServiceClient | None = None
        self._container: str = ""
        self._prefix: str = ""
        self._pending: list[DbChangeEvent] = []

    def name(self) -> str:
        return "Azure Blob Storage"

    def description(self) -> str:
        return "Stream CDC events to Azure Blob Storage as JSON objects."

    def example_url(self) -> str:
        return "azureblob://accountname/containername?key=<base64-key>"

    def configuration(self) -> list[DriverField]:
        return [
            required_string("AccountName", "Storage account name"),
            required_string("ContainerName", "Blob container name"),
            optional_string("Prefix", "Blob path prefix"),
            optional_password("AccountKey", "Storage account key (base64)"),
            optional_string("ConnectionString", "Full connection string (overrides key)"),
        ]

    def validate(self, values: dict[str, Any]) -> tuple[str, list[FieldError]]:
        errors: list[FieldError] = []
        account = get_str(values, "AccountName")
        container = get_str(values, "ContainerName")
        if not account:
            errors.append(FieldError("AccountName", "Account name is required"))
        if not container:
            errors.append(FieldError("ContainerName", "Container name is required"))
        if errors:
            return "", errors
        prefix = get_str(values, "Prefix")
        key = get_str(values, "AccountKey")
        conn = get_str(values, "ConnectionString")
        path = f"/{container}/{prefix}" if prefix else f"/{container}"
        qs = f"?key={key}" if key else (f"?connection-string={conn}" if conn else "")
        return f"azureblob://{account}{path}{qs}", []

    def max_batch_size(self) -> int:
        return 500

    async def start(self, config: DriverConfig) -> None:
        u = urlparse(config.url)
        qs = parse_qs(u.query)
        account = u.hostname or ""
        path_parts = u.path.lstrip("/").split("/", 1)
        self._container = path_parts[0] if path_parts else ""
        self._prefix = path_parts[1] if len(path_parts) > 1 else ""

        conn_str = unquote((qs.get("connection-string") or [""])[0])
        key = unquote((qs.get("key") or [""])[0])

        if conn_str:
            self._client = BlobServiceClient.from_connection_string(conn_str)
        elif key:
            url = f"https://{account}.blob.core.windows.net"
            from azure.core.credentials import AzureNamedKeyCredential
            cred = AzureNamedKeyCredential(account, key)
            self._client = BlobServiceClient(account_url=url, credential=cred)
        else:
            from azure.identity.aio import DefaultAzureCredential
            url = f"https://{account}.blob.core.windows.net"
            self._client = BlobServiceClient(account_url=url, credential=DefaultAzureCredential())

    async def stop(self) -> None:
        await self.flush()
        if self._client:
            await self._client.close()

    async def process(self, event: DbChangeEvent) -> bool:
        self._pending.append(event)
        return False

    async def flush(self) -> None:
        if not self._pending or not self._client:
            return

        container = self._client.get_container_client(self._container)
        for evt in self._pending:
            parts = [self._prefix, evt.table, f"{evt.timestamp}-{evt.id}.json"]
            blob_name = "/".join(p for p in parts if p)
            body = json.dumps({
                "operation": evt.operation,
                "id": evt.id,
                "table": evt.table,
                "timestamp": evt.timestamp,
                "data": evt.get_object(),
            }).encode()
            blob = container.get_blob_client(blob_name)
            await blob.upload_blob(body, overwrite=True, content_settings=None)  # type: ignore[arg-type]

        self._pending = []

    async def test(self, url: str) -> None:
        import logging
        await self.start(DriverConfig(url=url, logger=logging.getLogger(__name__), data_dir=""))
        assert self._client
        cc = self._client.get_container_client(self._container)
        await cc.get_container_properties()
