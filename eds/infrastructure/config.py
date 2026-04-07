"""TOML + environment variable configuration.

Mirrors cmd/root.go config loading and EDS_* env-var overrides.
Environment variables prefixed with EDS_ override config.toml values.
"""

from __future__ import annotations

import os
import stat
import tomllib
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class MetricsConfig:
    port: int = 8080
    host: str = "localhost"


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
        cfg.events_schema = raw.get("events_schema", "")
        if "metrics" in raw:
            m = raw["metrics"]
            cfg.metrics.port = int(m.get("port", 8080))
            cfg.metrics.host = m.get("host", "localhost")

    # Environment overrides
    cfg.token = os.environ.get("EDS_TOKEN", cfg.token)
    cfg.server_id = os.environ.get("EDS_SERVER_ID", cfg.server_id)
    cfg.url = os.environ.get("EDS_URL", cfg.url)
    cfg.api_url = os.environ.get("EDS_API_URL", cfg.api_url)

    return cfg


def save_config(data_dir: str, token: str, server_id: str) -> None:
    """Write a minimal config.toml with restricted permissions (0600)."""
    path = Path(data_dir)
    path.mkdir(parents=True, exist_ok=True)
    config_path = path / "config.toml"
    content = f'token     = "{token}"\nserver_id = "{server_id}"\n'
    config_path.write_text(content)
    config_path.chmod(stat.S_IRUSR | stat.S_IWUSR)


async def set_config_value(data_dir: str, key: str, value: str) -> None:
    """Update a single key=value in config.toml (upsert)."""
    config_path = Path(data_dir) / "config.toml"
    lines: list[str] = []
    found = False
    if config_path.exists():
        for line in config_path.read_text().splitlines():
            stripped = line.strip()
            if stripped.startswith(f"{key}") and "=" in stripped:
                lines.append(f'{key} = "{value}"')
                found = True
            else:
                lines.append(line)
    if not found:
        lines.append(f'{key} = "{value}"')
    config_path.write_text("\n".join(lines) + "\n")
    config_path.chmod(stat.S_IRUSR | stat.S_IWUSR)
