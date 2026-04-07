"""Async retry helper with exponential back-off and jitter.

Mirrors EDS.Core.Utilities.RetryHelper from the .NET port and
internal/util/http.go from the Go implementation.
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Awaitable, Callable, TypeVar

T = TypeVar("T")

_TRANSIENT_STATUS = {429, 500, 502, 503, 504}

_log = logging.getLogger(__name__)


def is_transient(exc: Exception) -> bool:
    """Return True for errors that are worth retrying."""
    msg = str(exc).lower()
    return any(
        kw in msg
        for kw in (
            "connection reset",
            "connection refused",
            "temporarily unavailable",
            "timeout",
            "too many requests",
            "service unavailable",
            "bad gateway",
            "gateway timeout",
        )
    )


async def execute(
    fn: Callable[[], Awaitable[T]],
    logger: logging.Logger | None = None,
    operation_name: str = "operation",
    max_attempts: int = 10,
    base_delay: float = 0.1,
    max_delay: float = 30.0,
) -> T:
    """Execute *fn* with retry on transient errors.

    Uses exponential back-off with jitter:
        delay = min(base_delay * 2^attempt + random(0, 0.5 * attempt), max_delay)
    """
    log = logger or _log
    attempt = 0
    while True:
        try:
            return await fn()
        except Exception as exc:
            attempt += 1
            if attempt >= max_attempts or not is_transient(exc):
                raise
            delay = min(base_delay * (2 ** attempt) + random.uniform(0, 0.5 * attempt), max_delay)
            log.warning(
                "Transient error on %s (attempt %d/%d): %s — retrying in %.1fs",
                operation_name, attempt, max_attempts, exc, delay,
            )
            await asyncio.sleep(delay)
