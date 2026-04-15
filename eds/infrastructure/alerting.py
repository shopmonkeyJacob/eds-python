"""Outbound alert channels: Slack, PagerDuty, generic webhook, SMTP email.

Alert events are routed to all configured channels whose severity list includes
the alert's severity. A per-channel, per-title cooldown suppresses duplicates so
a rapid stream of flush retries cannot flood an on-call rotation.
"""

from __future__ import annotations

import asyncio
import logging
import smtplib
import time
from dataclasses import dataclass, field
from email.mime.text import MIMEText
from typing import Any

import aiohttp

_log = logging.getLogger(__name__)

SEVERITY_INFO     = "info"
SEVERITY_WARNING  = "warning"
SEVERITY_CRITICAL = "critical"


@dataclass
class Alert:
    """A single outbound alert event."""
    title: str
    body: str
    severity: str = SEVERITY_WARNING
    tags: dict[str, str] = field(default_factory=dict)


@dataclass
class ChannelConfig:
    """Configuration for one alert delivery channel."""
    type: str                                      # slack | pagerduty | webhook | email
    severity: list[str] = field(default_factory=lambda: [SEVERITY_WARNING, SEVERITY_CRITICAL])

    # Slack / generic webhook
    url: str = ""
    headers: dict[str, str] = field(default_factory=dict)

    # PagerDuty Events API v2
    routing_key: str = ""

    # Email (SMTP)
    host: str = ""
    port: int = 587
    username: str = ""
    password: str = ""
    from_addr: str = ""
    to_addrs: list[str] = field(default_factory=list)
    use_tls: bool = True


class AlertManager:
    """Routes alert events to configured channels with cooldown deduplication.

    The cooldown is keyed on (channel_index, alert_title) so the same alert
    title is rate-limited independently per channel. All channel deliveries for
    a single call to fire() run concurrently; failures are logged but never
    propagate to the caller.
    """

    def __init__(self, channels: list[ChannelConfig], cooldown_seconds: int = 300) -> None:
        self._channels = channels
        self._cooldown = cooldown_seconds
        # (channel_index, title) → monotonic timestamp of last delivery
        self._last_fired: dict[tuple[int, str], float] = {}

    async def fire(self, alert: Alert) -> None:
        """Send *alert* to every eligible channel, suppressing duplicates via cooldown."""
        if not self._channels:
            return
        now = time.monotonic()
        tasks: list[Any] = []
        for idx, ch in enumerate(self._channels):
            if alert.severity not in ch.severity:
                continue
            key = (idx, alert.title)
            last = self._last_fired.get(key, float("-inf"))
            if now - last < self._cooldown:
                _log.debug("[alerting] Suppressing '%s' on channel %d (cooldown).", alert.title, idx)
                continue
            self._last_fired[key] = now
            tasks.append(_send_to_channel(ch, alert))
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, Exception):
                    _log.error("[alerting] Channel delivery failed: %s", r)


# ── Internal channel senders ──────────────────────────────────────────────────

async def _send_to_channel(ch: ChannelConfig, alert: Alert) -> None:
    t = ch.type.lower()
    if t == "slack":
        await _send_slack(ch, alert)
    elif t == "pagerduty":
        await _send_pagerduty(ch, alert)
    elif t == "webhook":
        await _send_webhook(ch, alert)
    elif t == "email":
        await _send_email(ch, alert)
    else:
        _log.warning("[alerting] Unknown channel type '%s' — skipping.", ch.type)


_SLACK_EMOJI: dict[str, str] = {
    SEVERITY_INFO:     ":information_source:",
    SEVERITY_WARNING:  ":warning:",
    SEVERITY_CRITICAL: ":rotating_light:",
}


async def _send_slack(ch: ChannelConfig, alert: Alert) -> None:
    emoji = _SLACK_EMOJI.get(alert.severity, ":bell:")
    payload = {"text": f"{emoji} *{alert.title}*\n{alert.body}"}
    async with aiohttp.ClientSession() as s:
        async with s.post(ch.url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status >= 400:
                text = await resp.text()
                raise RuntimeError(f"Slack HTTP {resp.status}: {text[:200]}")
    _log.info("[alerting] Slack alert sent: %s", alert.title)


async def _send_pagerduty(ch: ChannelConfig, alert: Alert) -> None:
    payload = {
        "routing_key":  ch.routing_key,
        "event_action": "trigger",
        "payload": {
            "summary":        alert.title,
            "severity":       alert.severity,
            "source":         "eds",
            "custom_details": {"detail": alert.body, **alert.tags},
        },
    }
    async with aiohttp.ClientSession() as s:
        async with s.post(
            "https://events.pagerduty.com/v2/enqueue",
            json=payload,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status >= 400:
                text = await resp.text()
                raise RuntimeError(f"PagerDuty HTTP {resp.status}: {text[:200]}")
    _log.info("[alerting] PagerDuty alert sent: %s", alert.title)


async def _send_webhook(ch: ChannelConfig, alert: Alert) -> None:
    body: dict[str, Any] = {
        "title":    alert.title,
        "body":     alert.body,
        "severity": alert.severity,
        **alert.tags,
    }
    headers = {"Content-Type": "application/json", **ch.headers}
    async with aiohttp.ClientSession(headers=headers) as s:
        async with s.post(ch.url, json=body, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status >= 400:
                text = await resp.text()
                raise RuntimeError(f"Webhook HTTP {resp.status}: {text[:200]}")
    _log.info("[alerting] Webhook alert sent: %s", alert.title)


async def _send_email(ch: ChannelConfig, alert: Alert) -> None:
    msg = MIMEText(f"{alert.title}\n\n{alert.body}", "plain")
    msg["Subject"] = f"[EDS {alert.severity.upper()}] {alert.title}"
    msg["From"]    = ch.from_addr or ch.username
    msg["To"]      = ", ".join(ch.to_addrs)
    sender         = msg["From"]
    recipients     = list(ch.to_addrs)
    raw            = msg.as_string()

    def _smtp_send() -> None:
        with smtplib.SMTP(ch.host, ch.port) as smtp:
            if ch.use_tls:
                smtp.starttls()
            if ch.username:
                smtp.login(ch.username, ch.password)
            smtp.sendmail(sender, recipients, raw)

    await asyncio.to_thread(_smtp_send)
    _log.info("[alerting] Email alert sent: %s → %s", alert.title, ch.to_addrs)
