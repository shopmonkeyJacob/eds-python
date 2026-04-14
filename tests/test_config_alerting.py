"""Unit tests for [alerts] / [[alerts.channels]] parsing in load_config.

These are pure unit tests — they write a temp TOML file and verify the
resulting EdsConfig fields. No network or process isolation required.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from eds.infrastructure.config import load_config


_SAMPLE_TOML = """\
token     = "test-token"
server_id = "test-server"

[alerts]
cooldown_seconds = 120

[[alerts.channels]]
type     = "slack"
url      = "https://hooks.slack.com/test"
severity = ["critical"]

[[alerts.channels]]
type        = "pagerduty"
routing_key = "my-key"

[[alerts.channels]]
type     = "email"
host     = "smtp.example.com"
port     = 465
username = "eds@example.com"
password = "secret"
from     = "eds@example.com"
to       = ["oncall@example.com", "team@example.com"]
use_tls  = false
"""


def _load(toml: str):
    with tempfile.TemporaryDirectory() as d:
        Path(d, "config.toml").write_text(toml)
        return load_config(d)


# ── Top-level alerts section ──────────────────────────────────────────────────


def test_alerts_cooldown_seconds() -> None:
    assert _load(_SAMPLE_TOML).alerts.cooldown_seconds == 120


def test_alerts_channel_count() -> None:
    assert len(_load(_SAMPLE_TOML).alerts.channels) == 3


# ── Slack channel ─────────────────────────────────────────────────────────────


def test_slack_channel_type() -> None:
    ch = _load(_SAMPLE_TOML).alerts.channels[0]
    assert ch.type == "slack"


def test_slack_channel_url() -> None:
    ch = _load(_SAMPLE_TOML).alerts.channels[0]
    assert ch.url == "https://hooks.slack.com/test"


def test_slack_channel_severity() -> None:
    ch = _load(_SAMPLE_TOML).alerts.channels[0]
    assert ch.severity == ["critical"]


# ── PagerDuty channel ─────────────────────────────────────────────────────────


def test_pagerduty_routing_key() -> None:
    ch = _load(_SAMPLE_TOML).alerts.channels[1]
    assert ch.type == "pagerduty"
    assert ch.routing_key == "my-key"


def test_pagerduty_default_severity() -> None:
    # No severity specified → defaults to ["warning", "critical"]
    ch = _load(_SAMPLE_TOML).alerts.channels[1]
    assert ch.severity == ["warning", "critical"]


# ── Email channel ─────────────────────────────────────────────────────────────


def test_email_host_and_port() -> None:
    ch = _load(_SAMPLE_TOML).alerts.channels[2]
    assert ch.host == "smtp.example.com"
    assert ch.port == 465


def test_email_from_addr() -> None:
    # TOML key is "from" (reserved in Python) — mapped to from_addr
    ch = _load(_SAMPLE_TOML).alerts.channels[2]
    assert ch.from_addr == "eds@example.com"


def test_email_to_addrs() -> None:
    ch = _load(_SAMPLE_TOML).alerts.channels[2]
    assert ch.to_addrs == ["oncall@example.com", "team@example.com"]


def test_email_use_tls_false() -> None:
    ch = _load(_SAMPLE_TOML).alerts.channels[2]
    assert ch.use_tls is False


# ── Defaults when [alerts] is absent ─────────────────────────────────────────


def test_no_alerts_section_default_cooldown() -> None:
    cfg = _load('token = "t"\nserver_id = "s"\n')
    assert cfg.alerts.cooldown_seconds == 300


def test_no_alerts_section_empty_channels() -> None:
    cfg = _load('token = "t"\nserver_id = "s"\n')
    assert cfg.alerts.channels == []


# ── Webhook channel with headers ──────────────────────────────────────────────


def test_webhook_headers_parsed() -> None:
    toml = """\
[alerts]
[[alerts.channels]]
type = "webhook"
url  = "https://example.com/hook"
[alerts.channels.headers]
Authorization = "Bearer secret"
X-Custom = "value"
"""
    ch = _load(toml).alerts.channels[0]
    assert ch.headers == {"Authorization": "Bearer secret", "X-Custom": "value"}
