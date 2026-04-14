"""Prometheus metrics and HTTP health/status server."""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from urllib.parse import urlparse, urlunparse

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    CONTENT_TYPE_LATEST,
    generate_latest,
)

_log = logging.getLogger(__name__)

# ── Metric definitions ────────────────────────────────────────────────────────

pending_events: Gauge = Gauge(
    "eds_pending_events",
    "Number of events currently buffered and awaiting flush",
)

total_events: Counter = Counter(
    "eds_total_events",
    "Total number of events received from NATS",
)

flush_duration: Histogram = Histogram(
    "eds_flush_duration_seconds",
    "Duration of driver flush operations",
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

flush_count: Histogram = Histogram(
    "eds_flush_count",
    "Number of events flushed per batch",
    buckets=[1, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000],
)

processing_duration: Histogram = Histogram(
    "eds_processing_duration_seconds",
    "End-to-end event processing latency (NATS receive → ACK)",
    buckets=[1, 2, 5, 10, 30, 60, 120, 300, 600, 1800, 3600],
)


# ── Health / Status state ─────────────────────────────────────────────────────

def _sanitize_url(url: str) -> str:
    """Strip credentials from a driver URL for safe display."""
    if not url:
        return url
    try:
        p = urlparse(url)
        host = p.hostname or ""
        netloc = f"{host}:{p.port}" if p.port else host
        return urlunparse((p.scheme, netloc, p.path, p.params, p.query, p.fragment))
    except Exception:
        return url


class HealthState:
    """Thread-safe runtime state for /healthz probes and the /status endpoint.

    Updated from asyncio context (consumer, server) and read from the HTTP
    server daemon thread — all access is guarded by a threading.Lock.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._nats_connected = False
        self._consumer_running = False
        self._paused = False
        self._version = ""
        self._session_id = ""
        self._driver = ""
        self._started_at = time.monotonic()
        self._events_processed = 0
        self._pending_flush = 0
        self._last_event_at: datetime | None = None
        self._last_event_table = ""

    # ── Setters (called from asyncio / consumer context) ─────────────────────

    def set_nats_connected(self, v: bool) -> None:
        with self._lock:
            self._nats_connected = v

    def set_consumer_running(self, v: bool) -> None:
        with self._lock:
            self._consumer_running = v

    def set_paused(self, v: bool) -> None:
        with self._lock:
            self._paused = v

    def set_info(self, version: str, session_id: str, driver_url: str) -> None:
        """Called once per session with startup metadata."""
        with self._lock:
            self._version = version
            self._session_id = session_id
            self._driver = _sanitize_url(driver_url)

    def record_event(self, table: str) -> None:
        with self._lock:
            self._last_event_at = datetime.now(timezone.utc)
            self._last_event_table = table

    def record_flush(self, count: int) -> None:
        with self._lock:
            self._events_processed += count

    def set_pending_flush(self, count: int) -> None:
        with self._lock:
            self._pending_flush = count

    # ── Probe responses (called from HTTP server daemon thread) ───────────────

    def liveness(self) -> tuple[int, dict]:
        """200 while the consumer loop is running; 503 if it has stopped."""
        with self._lock:
            running = self._consumer_running
        status = "ok" if running else "unavailable"
        return (200 if running else 503), {
            "status": status,
            "checks": {"consumer": "ok" if running else "stopped"},
        }

    def readiness(self) -> tuple[int, dict]:
        """200 when NATS is connected and the consumer is active."""
        with self._lock:
            connected = self._nats_connected
            running = self._consumer_running
            paused = self._paused
        ok = connected and running
        return (200 if ok else 503), {
            "status": "ok" if ok else "unavailable",
            "checks": {
                "nats": "ok" if connected else "disconnected",
                "consumer": ("paused" if paused else "ok") if running else "stopped",
            },
        }

    def snapshot(self) -> dict:
        """Full status snapshot for the /status endpoint."""
        with self._lock:
            last_at = self._last_event_at
            return {
                "version": self._version,
                "session_id": self._session_id,
                "driver": self._driver,
                "uptime_seconds": int(time.monotonic() - self._started_at),
                "paused": self._paused,
                "last_event_at": last_at.isoformat() if last_at else None,
                "last_event_table": self._last_event_table,
                "events_processed": self._events_processed,
                "pending_flush": self._pending_flush,
            }


health = HealthState()


# ── HTTP server ───────────────────────────────────────────────────────────────

class _MetricsHandler(BaseHTTPRequestHandler):

    def log_message(self, fmt: str, *args: object) -> None:  # silence access log
        pass

    def do_GET(self) -> None:
        path = self.path.split("?", 1)[0].rstrip("/")

        if path in ("/metrics", ""):
            data = generate_latest()
            self._respond(200, CONTENT_TYPE_LATEST, data)

        elif path == "/status":
            body = json.dumps(health.snapshot(), default=str).encode()
            self._respond(200, "application/json; charset=utf-8", body)

        elif path == "/healthz/live":
            code, payload = health.liveness()
            self._respond(code, "application/json; charset=utf-8", json.dumps(payload).encode())

        elif path == "/healthz/ready":
            code, payload = health.readiness()
            self._respond(code, "application/json; charset=utf-8", json.dumps(payload).encode())

        else:
            self.send_response(404)
            self.end_headers()

    def _respond(self, code: int, content_type: str, body: bytes) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def start_metrics_server(host: str = "localhost", port: int = 8080) -> HTTPServer:
    """Start the metrics/health/status HTTP server in a daemon thread."""
    server = HTTPServer((host, port), _MetricsHandler)
    t = Thread(target=server.serve_forever, daemon=True, name="metrics-server")
    t.start()
    _log.info(
        "Metrics server listening on %s:%d  —  /metrics  /status  /healthz/live  /healthz/ready",
        host, port,
    )
    return server
