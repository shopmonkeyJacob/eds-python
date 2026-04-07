"""NATS notification handler — mirrors internal/notification/notification.go.

Subscribes to eds.notify.{session_id}.> and dispatches commands from HQ.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Awaitable, Callable

import msgpack
import nats
from nats.aio.client import Client as NatsClient
from nats.aio.msg import Msg

_log = logging.getLogger(__name__)

MAX_PAYLOAD_BYTES = 1 * 1024 * 1024  # 1 MB — DoS guard
MAX_CONCURRENT_HANDLERS = 8          # cap task fan-out


@dataclass
class NotificationHandlers:
    restart: Callable[[], Awaitable[None]] | None = None
    shutdown: Callable[[str, bool], Awaitable[None]] | None = None
    pause: Callable[[], Awaitable[None]] | None = None
    unpause: Callable[[], Awaitable[None]] | None = None
    upgrade: Callable[[str], Awaitable[tuple[bool, str | None]]] | None = None
    send_logs: Callable[[], Awaitable[tuple[bool, str | None]]] | None = None
    driver_config: Callable[[], dict] | None = None
    validate: Callable[[str, dict], dict] | None = None
    configure: Callable[[str, bool], Awaitable[dict]] | None = None
    import_data: Callable[[bool], Awaitable[None]] | None = None


class NotificationService:

    def __init__(
        self,
        nats_url: str,
        credentials_file: str,
        session_id: str,
        handlers: NotificationHandlers,
    ) -> None:
        self._nats_url = nats_url
        self._credentials_file = credentials_file
        self._session_id = session_id
        self._handlers = handlers
        self._nc: NatsClient | None = None
        self._sem = asyncio.Semaphore(MAX_CONCURRENT_HANDLERS)

    async def start(self) -> None:
        options: list = []
        if self._credentials_file:
            options.append(nats.credentials(self._credentials_file))

        self._nc = await nats.connect(self._nats_url, *options)
        subject = f"eds.notify.{self._session_id}.>"
        await self._nc.subscribe(subject, cb=self._on_message)
        _log.info("Notification service listening on %s", subject)

    async def stop(self) -> None:
        if self._nc:
            await self._nc.drain()

    async def _on_message(self, msg: Msg) -> None:
        parts = msg.subject.split(".")
        action = parts[3] if len(parts) >= 4 else "unknown"

        if msg.data and len(msg.data) > MAX_PAYLOAD_BYTES:
            _log.warning(
                "Dropping oversized notification '%s' (%d bytes)", action, len(msg.data)
            )
            return

        async def handle() -> None:
            async with self._sem:
                try:
                    await asyncio.wait_for(
                        self._dispatch(action, msg), timeout=300
                    )
                except asyncio.TimeoutError:
                    _log.warning("Notification handler '%s' timed out", action)
                except Exception as exc:
                    _log.error("Error handling notification '%s': %s", action, exc)

        asyncio.create_task(handle(), name=f"notify-{action}")

    async def _dispatch(self, action: str, msg: Msg) -> None:
        payload = self._parse_payload(msg)
        h = self._handlers
        nc = self._nc
        sid = self._session_id

        if action == "ping":
            reply_to = payload.get("subject")
            if reply_to and nc:
                await nc.publish(reply_to, b"pong")

        elif action == "driverconfig":
            if h.driver_config:
                resp = h.driver_config()
                await self._reply_msgpack(msg, resp)

        elif action == "validate":
            if h.validate:
                driver = payload.get("driver", "")
                cfg = payload.get("config", {})
                resp = h.validate(driver, cfg)
                await self._reply_msgpack(msg, resp)

        elif action == "configure":
            if h.configure:
                url = payload.get("url", "")
                backfill = bool(payload.get("backfill", False))
                resp = await h.configure(url, backfill)
                await self._reply_msgpack(msg, resp)

        elif action == "restart":
            if h.restart:
                await h.restart()
            await self._publish_response(action, {"success": True, "sessionId": sid, "action": action})

        elif action == "pause":
            if h.pause:
                await h.pause()
            await self._publish_response(action, {"success": True, "sessionId": sid, "action": action})

        elif action == "unpause":
            if h.unpause:
                await h.unpause()
            await self._publish_response(action, {"success": True, "sessionId": sid, "action": action})

        elif action == "shutdown":
            message = payload.get("message", "")
            deleted = bool(payload.get("deleted", False))
            if h.shutdown:
                await h.shutdown(message, deleted)

        elif action == "upgrade":
            version = payload.get("version")
            if version and h.upgrade:
                success, error = await h.upgrade(version)
                await self._publish_response(action, {
                    "success": success, "message": error,
                    "sessionId": sid, "action": action,
                })

        elif action == "sendlogs":
            if h.send_logs:
                success, error = await h.send_logs()
                await self._publish_response(action, {
                    "success": success, "message": error,
                    "sessionId": sid, "action": action,
                })

        elif action == "import":
            backfill = bool(payload.get("backfill", False))
            if h.import_data:
                await h.import_data(backfill)
            await self._publish_response(action, {"success": True, "sessionId": sid, "action": action})

        else:
            _log.warning("Unknown notification action: %s", action)

    def _parse_payload(self, msg: Msg) -> dict:
        if not msg.data:
            return {}
        try:
            ct = (msg.headers or {}).get("Content-Type", "")
            if isinstance(ct, list):
                ct = " ".join(ct)
            if "msgpack" in ct.lower():
                raw = msgpack.unpackb(msg.data, raw=False)
                return raw if isinstance(raw, dict) else {}
            return json.loads(msg.data.decode())
        except Exception:
            return {}

    async def _reply_msgpack(self, msg: Msg, payload: dict) -> None:
        data = msgpack.packb(payload, use_bin_type=True)
        await msg.respond(data, headers={"content-encoding": "msgpack"})

    async def _publish_response(self, action: str, payload: dict) -> None:
        if not self._nc:
            return
        subject = f"eds.client.{self._session_id}.{action}-response"
        data = msgpack.packb(payload, use_bin_type=True)
        await self._nc.publish(subject, data, headers={"content-encoding": "msgpack"})
