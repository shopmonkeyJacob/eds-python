"""NATS JetStream consumer — mirrors internal/consumer/consumer.go.

Architecture:
- asyncio task subscribes to JetStream messages and places them on an
  asyncio.Queue (the buffer).
- A separate asyncio task (the bufferer) drains the queue, calls
  driver.process(), and flushes when batch conditions are met.
- Heartbeats are sent every heartbeat_interval (default 60 s) on a
  third asyncio task.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import msgpack
import nats
from nats.aio.client import Client as NatsClient
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig, AckPolicy, DeliverPolicy

from eds.core.driver import Driver
from eds.core.models import DbChangeEvent
from eds.infrastructure import metrics
from eds.infrastructure.schema_registry import SchemaRegistry

_log = logging.getLogger(__name__)

DEFAULT_MIN_PENDING_LATENCY = 2.0   # seconds
DEFAULT_MAX_PENDING_LATENCY = 30.0  # seconds
DEFAULT_HEARTBEAT_INTERVAL = 60.0   # seconds
DEFAULT_MAX_ACK_PENDING = 25_000
EMPTY_BUFFER_PAUSE = 0.01           # seconds — prevents CPU spin


@dataclass
class ConsumerConfig_:
    """Configuration for the NATS JetStream consumer."""
    nats_url: str
    credentials_file: str
    session_id: str
    server_id: str
    company_ids: list[str]
    driver: Driver
    api_key: str
    registry: SchemaRegistry | None = None
    export_table_timestamps: dict[str, datetime] | None = None
    deliver_all: bool = False
    max_ack_pending: int = DEFAULT_MAX_ACK_PENDING
    heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL
    min_pending_latency: float = DEFAULT_MIN_PENDING_LATENCY
    max_pending_latency: float = DEFAULT_MAX_PENDING_LATENCY


class Consumer:
    """NATS JetStream consumer with batching, back-pressure, and heartbeats."""

    def __init__(self, config: ConsumerConfig_) -> None:
        self._config = config
        self._nc: NatsClient | None = None
        self._js: JetStreamContext | None = None
        self._subscription: Any = None

        self._buffer: asyncio.Queue[Any] = asyncio.Queue(maxsize=config.max_ack_pending)
        self._pending: list[tuple[Any, DbChangeEvent, float]] = []  # (msg, event, received_ms)
        self._pending_started: float | None = None
        self._pause_started: datetime | None = None
        self._started: float = time.monotonic()
        self._offset: int = 0
        self._sequence: int = 0
        self._stopping = False
        self._disconnected: asyncio.Event = asyncio.Event()

        self._js_cfg: ConsumerConfig | None = None
        self._bufferer_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None

    @property
    def disconnected(self) -> asyncio.Event:
        return self._disconnected

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        cfg = self._config
        def _on_closed(nc: NatsClient) -> None:
            if not self._stopping:
                _log.warning("NATS connection closed unexpectedly")
                self._disconnected.set()

        def _on_disconnect(nc: NatsClient) -> None:
            if not self._stopping:
                _log.error("NATS disconnected")
                self._disconnected.set()

        self._nc = await nats.connect(
            cfg.nats_url,
            user_credentials=cfg.credentials_file or None,
            closed_cb=_on_closed,
            disconnected_cb=_on_disconnect,
            name=f"eds-{cfg.server_id}",
        )
        self._js = self._nc.jetstream()

        consumer_name = f"eds-{cfg.server_id}"
        subjects = [
            f"dbchange.*.*.{cid}.*.PUBLIC.>" for cid in cfg.company_ids
        ]

        js_cfg = ConsumerConfig(
            durable_name=consumer_name,
            max_ack_pending=cfg.max_ack_pending,
            max_deliver=20,
            ack_wait=300,           # 5 minutes in seconds
            filter_subjects=subjects,
            ack_policy=AckPolicy.EXPLICIT,
        )

        # Determine start position
        deliver_all = cfg.deliver_all
        start_time: datetime | None = None
        if cfg.export_table_timestamps:
            times = list(cfg.export_table_timestamps.values())
            if times:
                start_time = min(times)

        try:
            consumer = await self._js.consumer_info("dbchange", consumer_name)
            js_cfg.deliver_policy = consumer.config.deliver_policy
            js_cfg.opt_start_time = consumer.config.opt_start_time
        except Exception:
            if deliver_all:
                js_cfg.deliver_policy = DeliverPolicy.ALL
            elif start_time:
                js_cfg.deliver_policy = DeliverPolicy.BY_START_TIME
                js_cfg.opt_start_time = start_time
            else:
                js_cfg.deliver_policy = DeliverPolicy.NEW
                _log.warning("No import timestamp found — starting stream from now")

        self._js_cfg = js_cfg
        try:
            self._subscription = await self._js.subscribe_bind(
                "dbchange", js_cfg, consumer_name, cb=self._on_message
            )
        except Exception:
            # Create consumer then subscribe
            await self._js.add_consumer("dbchange", js_cfg)
            self._subscription = await self._js.subscribe_bind(
                "dbchange", js_cfg, consumer_name, cb=self._on_message
            )

        self._bufferer_task = asyncio.create_task(self._bufferer(), name="eds-bufferer")
        self._heartbeat_task = asyncio.create_task(self._heartbeats(), name="eds-heartbeat")
        _log.info("NATS consumer started: %s", self._nc.connected_url)

    async def stop(self) -> None:
        self._stopping = True
        await self._flush()
        if self._subscription:
            await self._subscription.drain()
        if self._bufferer_task:
            self._bufferer_task.cancel()
            await asyncio.gather(self._bufferer_task, return_exceptions=True)
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            await asyncio.gather(self._heartbeat_task, return_exceptions=True)
        if self._nc:
            await self._nc.drain()
        _log.info("NATS consumer stopped")

    async def pause(self) -> None:
        if self._subscription:
            await self._subscription.drain()
            self._subscription = None
        self._pause_started = datetime.now(timezone.utc)
        _log.info("Consumer paused")

    async def unpause(self) -> None:
        cfg = self._config
        consumer_name = f"eds-{cfg.server_id}"
        assert self._js is not None
        assert self._js_cfg is not None
        self._subscription = await self._js.subscribe_bind(
            "dbchange", self._js_cfg, consumer_name, cb=self._on_message
        )
        self._pause_started = None
        _log.info("Consumer unpaused")

    # ── Message handling ───────────────────────────────────────────────────────

    async def _on_message(self, msg: Any) -> None:
        metrics.pending_events.inc()
        metrics.total_events.inc()
        await self._buffer.put(msg)

    async def _bufferer(self) -> None:
        cfg = self._config
        max_batch = cfg.max_ack_pending
        driver = cfg.driver

        while not self._stopping:
            try:
                msg = self._buffer.get_nowait()
            except asyncio.QueueEmpty:
                # Min-latency flush
                if (
                    self._pending
                    and self._pending_started is not None
                    and time.monotonic() - self._pending_started >= cfg.min_pending_latency
                ):
                    await self._flush()
                await asyncio.sleep(EMPTY_BUFFER_PAUSE)
                continue

            received_ms = time.monotonic() * 1000
            try:
                data = msg.data
                headers = msg.headers or {}
                ct = headers.get("Content-Type") or headers.get("content-type") or ""
                ce = headers.get("content-encoding") or headers.get("Content-Encoding") or ""
                if "msgpack" in ct or "msgpack" in ce:
                    payload = msgpack.unpackb(data, raw=False)
                else:
                    payload = json.loads(data)
                evt = DbChangeEvent.from_dict(payload)
            except Exception as exc:
                _log.error("Failed to decode message: %s", exc)
                await self._nack_everything()
                self._disconnected.set()
                return

            # MVCC timestamp filtering — skip events from before the import cutoff
            if cfg.export_table_timestamps:
                cutoff = cfg.export_table_timestamps.get(evt.table)
                if cutoff and evt.timestamp <= int(cutoff.timestamp() * 1000):
                    await msg.ack()
                    metrics.pending_events.dec()
                    continue

            self._pending.append((msg, evt, received_ms))
            if self._pending_started is None:
                self._pending_started = time.monotonic()

            force_flush, err = False, None
            try:
                await self._handle_possible_migration(evt)
                force_flush = await driver.process(evt)
            except Exception as exc:
                err = exc

            if err:
                _log.error("Driver.process error: %s", err)
                await self._nack_everything()
                self._disconnected.set()
                return

            driver_max = driver.max_batch_size()
            effective_max = driver_max if driver_max > 0 else max_batch

            should_flush = (
                force_flush
                or len(self._pending) >= effective_max
                or (
                    self._pending_started is not None
                    and time.monotonic() - self._pending_started >= cfg.max_pending_latency
                )
            )
            if should_flush:
                await self._flush()

    async def _flush(self) -> None:
        if not self._pending:
            return
        t0 = time.monotonic()
        try:
            await self._config.driver.flush()
        except Exception as exc:
            _log.error("Driver.flush error: %s", exc)
            await self._nack_everything()
            return

        acked_ms = time.monotonic() * 1000
        for msg, _evt, received_ms in self._pending:
            try:
                await msg.ack()
            except Exception as exc:
                _log.error("ACK error: %s", exc)
            metrics.pending_events.dec()
            metrics.processing_duration.observe((acked_ms - received_ms) / 1000.0)

        metrics.flush_duration.observe(time.monotonic() - t0)
        metrics.flush_count.observe(len(self._pending))
        self._pending = []
        self._pending_started = None

    # ── Schema migration (mirrors Go/C# handlePossibleMigration) ─────────────

    async def _handle_possible_migration(self, evt: DbChangeEvent) -> None:
        """Check whether the event's model version requires a schema migration.

        Raises on any failure so the caller's except block will NAK the event
        and retry — this is Gap 2: HQ unreachable / schema unavailable → NAK.
        """
        registry = self._config.registry
        driver   = self._config.driver
        if registry is None or not driver.supports_migration():
            return

        found, current_version = await registry.get_table_version(evt.table)
        if found and current_version == (evt.model_version or ""):
            return  # fast path: version unchanged

        model_version = evt.model_version or ""
        # Raises if HQ is unreachable — causes NAK via the caller's error handler.
        new_schema = await registry.get_schema(evt.table, model_version)

        if not found:
            _log.info("[consumer] Migrating new table: %s v%s", evt.table, model_version)
            await driver.migrate_new_table(new_schema)
            await registry.set_table_version(evt.table, model_version)
            return

        # Version changed — diff old schema vs new
        try:
            old_schema = await registry.get_schema(evt.table, current_version)
        except Exception:
            old_schema = None

        if old_schema is not None:
            old_names = {c.name for c in old_schema.column_defs()}
            new_cols = [c.name for c in new_schema.column_defs() if c.name not in old_names]
            if new_cols:
                _log.info("[consumer] Adding %d column(s) to %s: %s",
                          len(new_cols), evt.table, new_cols)
                await driver.migrate_new_columns(new_schema, new_cols)

            # Gap 3: detect type changes
            old_type_map = {c.name: c.data_type for c in old_schema.column_defs()}
            changed_cols = [
                c.name for c in new_schema.column_defs()
                if c.name in old_type_map and old_type_map[c.name] != c.data_type
            ]
            if changed_cols:
                _log.info("[consumer] Changing %d column type(s) on %s: %s",
                          len(changed_cols), evt.table, changed_cols)
                await driver.migrate_changed_columns(new_schema, changed_cols)

            # Gap 4: detect removed columns
            new_names = {c.name for c in new_schema.column_defs()}
            removed_cols = [c.name for c in old_schema.column_defs() if c.name not in new_names]
            if removed_cols:
                _log.info("[consumer] Dropping %d column(s) from %s: %s",
                          len(removed_cols), evt.table, removed_cols)
                await driver.migrate_removed_columns(new_schema, removed_cols)

        await registry.set_table_version(evt.table, model_version)

    async def _nack_everything(self) -> None:
        for msg, _evt, _ms in self._pending:
            try:
                await msg.nak()
            except Exception:
                pass
        self._pending = []
        self._pending_started = None

    # ── Heartbeat ─────────────────────────────────────────────────────────────

    async def _heartbeats(self) -> None:
        await self._send_heartbeat()
        while not self._stopping:
            await asyncio.sleep(self._config.heartbeat_interval)
            if self._stopping:
                break
            await self._send_heartbeat()

    async def _send_heartbeat(self) -> None:
        if not self._nc:
            return
        try:
            import psutil
            proc = psutil.Process()
            cpu = psutil.cpu_percent(interval=None)
            mem = proc.memory_info().rss
        except ImportError:
            _log.debug("psutil not available; heartbeat metrics will be zero")
            cpu, mem = 0.0, 0
        except Exception as exc:
            _log.debug("Failed to gather process metrics for heartbeat: %s", exc)
            cpu, mem = 0.0, 0

        hb = {
            "sessionId": self._config.session_id,
            "offset": self._offset,
            "uptime": time.monotonic() - self._started,
            "paused": self._pause_started.isoformat() if self._pause_started else None,
            "stats": {
                "cpu": cpu,
                "memory": mem,
                "hostname": socket.gethostname(),
                "os": os.name,
            },
        }
        self._offset += 1
        subject = f"eds.client.{self._config.session_id}.heartbeat"
        data = msgpack.packb(hb, use_bin_type=True)
        try:
            await self._nc.publish(subject, data, headers={"content-encoding": "msgpack"})
        except Exception as exc:
            _log.warning("Heartbeat error: %s", exc)
