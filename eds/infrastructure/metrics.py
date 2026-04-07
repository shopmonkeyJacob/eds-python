"""Prometheus metrics — mirrors internal/metrics.go."""

from __future__ import annotations

import asyncio
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

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


# ── HTTP server ───────────────────────────────────────────────────────────────

class _MetricsHandler(BaseHTTPRequestHandler):

    def log_message(self, fmt: str, *args: object) -> None:  # silence access log
        pass

    def do_GET(self) -> None:
        if self.path.rstrip("/") in ("/metrics", ""):
            data = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        else:
            self.send_response(404)
            self.end_headers()


def start_metrics_server(host: str = "localhost", port: int = 8080) -> HTTPServer:
    """Start the Prometheus /metrics endpoint in a daemon thread."""
    server = HTTPServer((host, port), _MetricsHandler)
    t = Thread(target=server.serve_forever, daemon=True, name="metrics-server")
    t.start()
    _log.info("Metrics server listening on %s:%d/metrics", host, port)
    return server
