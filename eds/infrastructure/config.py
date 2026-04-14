"""TOML + environment variable configuration.

Mirrors cmd/root.go config loading and EDS_* env-var overrides.
Environment variables prefixed with EDS_ override config.toml values.
"""

from __future__ import annotations

import os
import re
import stat
import tomllib
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class MetricsConfig:
    port: int = 8080
    host: str = "localhost"


@dataclass
class AlertChannelConfig:
    """Configuration for one outbound alert channel, parsed from [[alerts.channels]]."""
    type: str = ""                                                # slack | pagerduty | webhook | email
    severity: list[str] = field(default_factory=lambda: ["warning", "critical"])

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
    from_addr: str = ""                                           # mapped from TOML key "from"
    to_addrs: list[str] = field(default_factory=list)            # mapped from TOML key "to"
    use_tls: bool = True


@dataclass
class AlertsConfig:
    """Parsed from the [alerts] section of config.toml."""
    cooldown_seconds: int = 300
    channels: list[AlertChannelConfig] = field(default_factory=list)


@dataclass
class EdsConfig:
    token: str = ""
    server_id: str = ""
    url: str = ""          # driver URL
    api_url: str = "https://api.shopmonkey.cloud"
    keep_logs: bool = False
    metrics: MetricsConfig = field(default_factory=MetricsConfig)
    driver_mode: str = ""        # "upsert" | "timeseries" — empty means use default
    events_schema: str = ""      # events schema name — empty means use default

    alerts: AlertsConfig = field(default_factory=AlertsConfig)

    # Runtime — not persisted
    data_dir: str = "data"
    verbose: bool = False


def load_config(data_dir: str) -> EdsConfig:
    """Load config.toml from *data_dir*, then apply EDS_* env overrides."""
    cfg = EdsConfig(data_dir=data_dir)
    config_path = Path(data_dir) / "config.toml"

    if config_path.exists():
        with open(config_path, "rb") as f:
            raw = tomllib.load(f)
        cfg.token = raw.get("token", "")
        cfg.server_id = raw.get("server_id", "")
        cfg.url = raw.get("url", "")
        cfg.api_url = raw.get("api_url", cfg.api_url)
        cfg.keep_logs = bool(raw.get("keep_logs", False))
        cfg.driver_mode = raw.get("driver_mode", "")
        raw_schema = raw.get("events_schema", "")
        if raw_schema and not re.match(r'^[A-Za-z0-9_]{1,128}$', raw_schema):
            raise ValueError(
                f"Invalid events_schema name {raw_schema!r} in config.toml. "
                "Must contain only ASCII letters, digits, and underscores (1–128 chars)."
            )
        cfg.events_schema = raw_schema
        if "metrics" in raw:
            m = raw["metrics"]
            cfg.metrics.port = int(m.get("port", 8080))
            cfg.metrics.host = m.get("host", "localhost")
        if "alerts" in raw:
            a = raw["alerts"]
            cfg.alerts.cooldown_seconds = int(a.get("cooldown_seconds", 300))
            for ch_raw in a.get("channels", []):
                cfg.alerts.channels.append(AlertChannelConfig(
                    type=ch_raw.get("type", ""),
                    severity=list(ch_raw.get("severity", ["warning", "critical"])),
                    url=ch_raw.get("url", ""),
                    headers=dict(ch_raw.get("headers", {})),
                    routing_key=ch_raw.get("routing_key", ""),
                    host=ch_raw.get("host", ""),
                    port=int(ch_raw.get("port", 587)),
                    username=ch_raw.get("username", ""),
                    password=ch_raw.get("password", ""),
                    from_addr=ch_raw.get("from", ""),
                    to_addrs=list(ch_raw.get("to", [])),
                    use_tls=bool(ch_raw.get("use_tls", True)),
                ))

    # Environment overrides
    cfg.token = os.environ.get("EDS_TOKEN", cfg.token)
    cfg.server_id = os.environ.get("EDS_SERVER_ID", cfg.server_id)
    cfg.url = os.environ.get("EDS_URL", cfg.url)
    cfg.api_url = os.environ.get("EDS_API_URL", cfg.api_url)

    return cfg


def save_config(data_dir: str, token: str, server_id: str) -> None:
    """Write a minimal config.toml with restricted permissions (0600).

    Uses os.open() with O_CREAT so the file is never world-readable,
    even briefly.  There is no write-then-chmod race window.
    """
    path = Path(data_dir)
    path.mkdir(parents=True, exist_ok=True)
    config_path = path / "config.toml"
    content = f'token     = "{token}"\nserver_id = "{server_id}"\n'
    fd = os.open(config_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
                 stat.S_IRUSR | stat.S_IWUSR)
    try:
        os.write(fd, content.encode())
    finally:
        os.close(fd)


async def set_config_value(data_dir: str, key: str, value: str) -> None:
    """Update a single key=value in config.toml (upsert).

    Writes via a sibling temp file so the existing file's permissions are
    preserved and there is no window where the file has wrong permissions.
    """
    config_path = Path(data_dir) / "config.toml"
    tmp_path = config_path.with_suffix(".toml.tmp")
    lines: list[str] = []
    found = False
    if config_path.exists():
        for line in config_path.read_text().splitlines():
            stripped = line.strip()
            if re.match(rf'^{re.escape(key)}\s*=', stripped):
                lines.append(f'{key} = "{value}"')
                found = True
            else:
                lines.append(line)
    if not found:
        lines.append(f'{key} = "{value}"')
    content = ("\n".join(lines) + "\n").encode()
    fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
                 stat.S_IRUSR | stat.S_IWUSR)
    try:
        os.write(fd, content)
    finally:
        os.close(fd)
    os.replace(tmp_path, config_path)
