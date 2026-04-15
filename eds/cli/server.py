"""eds server — main streaming server command."""

from __future__ import annotations

import asyncio
import gzip
import logging
import os
import random
import shutil
import signal
import sys
from pathlib import Path

from typing import Any

import aiohttp
import click

from eds.exit_codes import ExitCodes
from eds.infrastructure.alerting import Alert, AlertManager, ChannelConfig, SEVERITY_CRITICAL
from eds.infrastructure.config import EdsConfig, load_config, set_config_value
from eds.infrastructure.consumer import Consumer, ConsumerConfig_, DEFAULT_MIN_PENDING_LATENCY, DEFAULT_MAX_PENDING_LATENCY, DEFAULT_MAX_ACK_PENDING
from eds.infrastructure.metrics import start_metrics_server, health
from eds.infrastructure.notification import NotificationHandlers, NotificationService
from eds.infrastructure.tracker import SqliteTracker
from eds.infrastructure.schema_registry import SchemaRegistry
from eds.infrastructure.upgrade import upgrade, validate_version_string
from eds.core.driver import new_driver, NullDriver, Driver, DriverConfig, DriverMode, get_driver_metadata, _registry
from eds.version import CURRENT

import eds.drivers  # noqa: F401

_log = logging.getLogger(__name__)

_DEFAULT_API_URL = "https://api.shopmonkey.cloud"
_RENEW_INTERVAL = 60 * 60 * 24  # 24 hours

# Startup retry parameters (mirrors Go's util.NewHTTPRetry / maxFailures=5)
_MAX_STARTUP_FAILURES   = 5
_STARTUP_RETRY_BASE_S   = 0.1   # 100 ms base delay
_STARTUP_RETRY_JITTER_S = 0.5   # up to 500 ms × attempt number
_ALREADY_RUNNING_DELAY_S = 5.0  # wait when HQ reports session already running
_NATS_RETRY_DELAY_S      = 5.0  # fixed delay between NATS connection retries


@click.command()
@click.option("--api-key", envvar="EDS_TOKEN", default="", help="Shopmonkey API key.")
@click.option("--api-url", default=_DEFAULT_API_URL, hidden=True)
@click.option("--url", envvar="EDS_URL", default="", help="Driver connection URL.")
@click.option("--driver-mode", "driver_mode", default=None,
              type=click.Choice(["upsert", "timeseries"], case_sensitive=False),
              help="Event writing mode: timeseries (default) or upsert.")
@click.option("--events-schema", "events_schema", default=None,
              help="Schema name for time-series events tables (default: eds_events).")
@click.option("--renew-interval", default=_RENEW_INTERVAL, hidden=True,
              help="Session renewal interval in seconds.")
@click.option("--dry-run", "dry_run", is_flag=True, default=False,
              help="Receive and decode events without writing to a destination.")
@click.pass_context
def server(
    ctx: click.Context,
    api_key: str,
    api_url: str,
    url: str,
    driver_mode: str | None,
    events_schema: str | None,
    renew_interval: int,
    dry_run: bool,
) -> None:
    """Start the EDS streaming server."""
    data_dir: str = ctx.obj["data_dir"]
    try:
        asyncio.run(_server(data_dir, api_key, api_url, url, driver_mode, events_schema, renew_interval, dry_run))
    except KeyboardInterrupt:
        pass


async def _server(
    data_dir: str,
    api_key: str,
    api_url: str,
    url: str,
    driver_mode_flag: str | None,
    events_schema_flag: str | None,
    renew_interval: int,
    dry_run: bool = False,
) -> None:
    cfg = load_config(data_dir)
    api_key = api_key or cfg.token
    api_url = api_url or cfg.api_url
    url = url or cfg.url

    if not api_key:
        raise click.ClickException(
            "API key not found. Run `eds enroll` or set EDS_TOKEN."
        )

    if dry_run:
        _log.warning(
            "[dry-run] Running in dry-run mode — events will be received and decoded "
            "but NOT written to any destination."
        )

    # ── Post-enrollment configuration wizard ────────────────────────────────
    # A fresh enrollment leaves config.toml with just token + server_id.
    # If driver_mode is not yet set (and no --driver-mode flag was passed),
    # the user hasn't been through the wizard yet — run it now.
    just_enrolled = not cfg.driver_mode and driver_mode_flag is None
    if just_enrolled:
        from eds.cli.config_wizard import run_wizard
        wizard_result = await run_wizard(data_dir)
        mode = wizard_result.mode
        events_schema = wizard_result.events_schema
        # Reload config so the consumer picks up the wizard-written values
        cfg = load_config(data_dir)
    else:
        # Resolve driver mode (flag vs config, persist if changed, prompt on conflict)
        mode = await _resolve_driver_mode(driver_mode_flag, cfg, data_dir)
        events_schema = await _resolve_events_schema(events_schema_flag, cfg, data_dir)

    # Read flush tuning from config (0 = use consumer defaults)
    min_pending_latency = (
        float(cfg.min_pending_latency) if cfg.min_pending_latency > 0
        else DEFAULT_MIN_PENDING_LATENCY
    )
    max_pending_latency = (
        float(cfg.max_pending_latency) if cfg.max_pending_latency > 0
        else DEFAULT_MAX_PENDING_LATENCY
    )
    max_ack_pending = (
        cfg.max_ack_pending if cfg.max_ack_pending > 0
        else DEFAULT_MAX_ACK_PENDING
    )

    # ── One-time setup (shared across session renewals) ───────────────────────
    tracker = SqliteTracker(Path(data_dir) / "state.db")
    await tracker.open()

    alerter: AlertManager | None = _build_alerter(cfg)

    start_metrics_server(host=cfg.metrics.host, port=cfg.metrics.port)

    # OS signal handler: sets a global stop event so the restart loop exits cleanly.
    loop = asyncio.get_event_loop()
    global_stop = asyncio.Event()
    exit_code = ExitCodes.SUCCESS

    def _handle_signal() -> None:
        nonlocal exit_code
        _log.info("Signal received — shutting down")
        exit_code = ExitCodes.SUCCESS
        global_stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            pass  # Windows

    # ── Session restart loop ──────────────────────────────────────────────────
    # Each iteration is one HQ session. On INTENTIONAL_RESTART (renewal timer or
    # HQ restart notification) the session ends and a new one begins in-process.
    # Upgrade sets exit_process=True so the loop breaks after session end,
    # allowing the replacement binary to be executed.
    try:
        while not global_stop.is_set():
            # Start session with HQ
            session_info = await _start_session(api_url, api_key, cfg.server_id, data_dir)
            session_id: str = session_info["sessionId"]
            nats_url: str = session_info["natsUrl"]
            creds_file: str = session_info["credentialsFile"]
            company_ids: list[str] = session_info["companyIds"]

            _log.info("Session started: %s", session_id)

            # Per-session resources
            registry = SchemaRegistry(api_url=api_url, api_key=api_key, tracker=tracker)

            driver: Driver | None = None
            if dry_run:
                driver = NullDriver()
                _log.info("[dry-run] NullDriver active — no writes will occur.")
            elif url:
                driver_cfg = DriverConfig(
                    url=url,
                    logger=logging.getLogger("driver"),
                    data_dir=data_dir,
                    tracker=tracker,
                    schema_registry=registry,
                    mode=mode,
                    events_schema=events_schema,
                )
                driver = await new_driver(url, driver_cfg)
            else:
                _log.warning("No driver URL configured — events will be received but not written")

            # Load MVCC timestamps from last import
            from eds.importer.importer import load_table_export_info
            export_infos = await load_table_export_info(tracker)
            export_timestamps = (
                {info.table: info.timestamp for info in export_infos}
                if export_infos else None
            )

            stop_event = asyncio.Event()
            session_exit_code = ExitCodes.SUCCESS
            exit_process = False

            consumer_cfg = ConsumerConfig_(
                nats_url=nats_url,
                credentials_file=creds_file,
                session_id=session_id,
                server_id=cfg.server_id,
                company_ids=company_ids,
                driver=driver,  # type: ignore[arg-type]
                api_key=api_key,
                registry=registry,
                export_table_timestamps=export_timestamps,
                tracker=tracker,
                alerter=alerter,
                min_pending_latency=min_pending_latency,
                max_pending_latency=max_pending_latency,
                max_ack_pending=max_ack_pending,
            )
            consumer = Consumer(consumer_cfg)

            # ── Notification handlers ─────────────────────────────────────────

            async def _on_restart() -> None:
                nonlocal session_exit_code
                _log.info("[server] Restart requested by HQ — renewing session.")
                session_exit_code = ExitCodes.INTENTIONAL_RESTART
                stop_event.set()

            async def _on_shutdown(message: str, deleted: bool) -> None:
                nonlocal session_exit_code
                if deleted:
                    _log.warning("[server] Server removed from HQ: %s", message)
                else:
                    _log.info("[server] Shutdown requested by HQ: %s", message)
                session_exit_code = ExitCodes.SUCCESS
                stop_event.set()

            async def _on_pause() -> None:
                await consumer.pause()

            async def _on_unpause() -> None:
                await consumer.unpause()

            async def _on_upgrade(version: str) -> tuple[bool, str | None]:
                nonlocal session_exit_code, exit_process
                try:
                    validate_version_string(version)
                    from eds.public_key import PUBLIC_KEY
                    binary_url = f"{api_url}/v3/eds/download/{version}/{_platform_suffix()}"
                    sig_url = binary_url + ".sig"
                    destination = str(Path(sys.argv[0]).resolve())
                    await upgrade(binary_url, sig_url, PUBLIC_KEY, destination)
                    # exitProcess=True so the loop exits after session end,
                    # allowing the process manager to relaunch the new binary.
                    exit_process = True
                    session_exit_code = ExitCodes.INTENTIONAL_RESTART
                    stop_event.set()
                    return True, None
                except Exception as exc:
                    _log.error("Upgrade failed: %s", exc)
                    return False, str(exc)

            async def _on_configure(driver_url: str, backfill: bool) -> dict[str, Any]:
                nonlocal url, driver
                if dry_run:
                    return {"success": False, "error": "Cannot reconfigure driver in dry-run mode."}
                try:
                    await set_config_value(data_dir, "url", driver_url)
                    if driver:
                        await driver.stop()
                    new_driver_cfg = DriverConfig(
                        url=driver_url,
                        logger=logging.getLogger("driver"),
                        data_dir=data_dir,
                        tracker=tracker,
                        schema_registry=registry,
                        mode=mode,
                        events_schema=events_schema,
                    )
                    driver = await new_driver(driver_url, new_driver_cfg)
                    url = driver_url
                    stop_event.set()
                    return {"success": True}
                except Exception as exc:
                    return {"success": False, "error": str(exc)}

            def _driver_config() -> dict[str, Any]:
                return {"drivers": get_driver_metadata()}

            def _validate(scheme: str, values: dict[str, Any]) -> dict[str, Any]:
                d = _registry.get(scheme)
                if not d:
                    return {"success": False, "error": f"Unknown driver: {scheme}"}
                result_url, errors = d.validate(values)
                if errors:
                    return {"success": False, "errors": [{"field": e.field, "message": e.message} for e in errors]}
                return {"success": True, "url": result_url}

            handlers = NotificationHandlers(
                restart=_on_restart,
                shutdown=_on_shutdown,
                pause=_on_pause,
                unpause=_on_unpause,
                upgrade=_on_upgrade,
                send_logs=lambda: _send_logs_to_hq(api_url, api_key, session_id, data_dir),
                configure=_on_configure,
                driver_config=_driver_config,
                validate=_validate,
            )

            notify_svc = NotificationService(
                nats_url=nats_url,
                credentials_file=creds_file,
                session_id=session_id,
                handlers=handlers,
            )

            # ── Per-session background tasks ──────────────────────────────────

            async def _renewal_timer() -> None:
                nonlocal session_exit_code
                await asyncio.sleep(renew_interval)
                if not stop_event.is_set():
                    _log.info(
                        "[server] Renew interval elapsed (%dh) — renewing session.",
                        renew_interval // 3600,
                    )
                    session_exit_code = ExitCodes.INTENTIONAL_RESTART
                    stop_event.set()

            async def _watch_disconnect() -> None:
                nonlocal session_exit_code
                await consumer.disconnected.wait()
                if not stop_event.is_set():
                    _log.error("NATS disconnected — exiting")
                    if alerter:
                        try:
                            await alerter.fire(Alert(
                                title="EDS NATS connection lost",
                                body=(
                                    f"The EDS server lost its NATS connection and is restarting "
                                    f"the session (session={session_id})."
                                ),
                                severity=SEVERITY_CRITICAL,
                            ))
                        except Exception as exc:
                            _log.debug("[alerting] Alert fire failed: %s", exc)
                    session_exit_code = ExitCodes.NATS_DISCONNECTED
                    stop_event.set()

            async def _watch_global_stop() -> None:
                await global_stop.wait()
                stop_event.set()

            # Start session services — retry NATS connection on transient failures
            nats_failures = 0
            while True:
                try:
                    await consumer.start()
                    break
                except Exception as exc:
                    nats_failures += 1
                    if nats_failures >= _MAX_STARTUP_FAILURES:
                        _log.error(
                            "[server] NATS consumer start failed after %d attempts: %s",
                            nats_failures, exc,
                        )
                        raise
                    _log.warning(
                        "[server] NATS consumer start failed (attempt %d/%d): %s — retrying in %ds",
                        nats_failures, _MAX_STARTUP_FAILURES, exc, int(_NATS_RETRY_DELAY_S),
                    )
                    await asyncio.sleep(_NATS_RETRY_DELAY_S)

            health.set_info(CURRENT, session_id, url)
            health.set_consumer_running(True)
            await notify_svc.start()
            renewal_task = asyncio.create_task(_renewal_timer())
            disconnect_task = asyncio.create_task(_watch_disconnect())
            global_stop_task = asyncio.create_task(_watch_global_stop())

            _log.info("EDS server v%s running (session=%s, mode=%s)", CURRENT, session_id, mode.value)

            # Wait for any stop signal
            await stop_event.wait()

            # Graceful shutdown of per-session tasks
            renewal_task.cancel()
            disconnect_task.cancel()
            global_stop_task.cancel()
            await asyncio.gather(renewal_task, disconnect_task, global_stop_task, return_exceptions=True)

            await consumer.stop()
            await notify_svc.stop()
            if driver:
                await driver.stop()

            await _send_logs_to_hq(api_url, api_key, session_id, data_dir)
            await _end_session(api_url, api_key, session_id)
            _log.info("[server] session ended: %s", session_id)

            exit_code = session_exit_code

            # ── Decide: renew session or exit ─────────────────────────────────
            if global_stop.is_set() or exit_process:
                break

            if exit_code == ExitCodes.INTENTIONAL_RESTART:
                _log.info("[server] Restarting with fresh session...")
                exit_code = ExitCodes.SUCCESS
                continue

            # Any other exit code (success, fatal) — stop.
            break

    finally:
        await tracker.close()

    _log.info("EDS server stopped (exit code %d)", exit_code)
    sys.exit(exit_code)


# ── Alert manager factory ─────────────────────────────────────────────────────

def _build_alerter(cfg: EdsConfig) -> AlertManager | None:
    """Build an AlertManager from config, or return None if no channels are configured."""
    if not cfg.alerts.channels:
        return None
    channels = [
        ChannelConfig(
            type=ch.type,
            severity=ch.severity,
            url=ch.url,
            headers=ch.headers,
            routing_key=ch.routing_key,
            host=ch.host,
            port=ch.port,
            username=ch.username,
            password=ch.password,
            from_addr=ch.from_addr,
            to_addrs=ch.to_addrs,
            use_tls=ch.use_tls,
        )
        for ch in cfg.alerts.channels
    ]
    return AlertManager(channels, cooldown_seconds=cfg.alerts.cooldown_seconds)


# ── Driver-mode resolution ────────────────────────────────────────────────────

async def _resolve_driver_mode(
    flag_value: str | None,
    cfg: EdsConfig,
    data_dir: str,
    no_confirm: bool = False,
) -> DriverMode:
    """Resolve driver mode: flag vs config, persist to config.toml if changed,
    prompt interactively on conflict."""
    if flag_value is None:
        return DriverMode(cfg.driver_mode) if cfg.driver_mode else DriverMode.TIMESERIES

    flag_mode = DriverMode(flag_value.lower())
    stored = cfg.driver_mode

    if stored and stored != flag_mode.value:
        if not no_confirm:
            _log.warning(
                "[config] Conflict: config.toml has driver_mode=%s but --driver-mode=%s was passed.",
                stored, flag_mode.value,
            )
            confirmed = click.confirm(
                f"config.toml has driver_mode={stored!r} but you passed "
                f"--driver-mode={flag_mode.value!r}. Change it?",
                default=False,
            )
            if not confirmed:
                _log.info("[config] Keeping existing driver_mode=%s.", stored)
                return DriverMode(stored)
        _log.info("[config] Updating driver_mode from %s to %s.", stored, flag_mode.value)

    await set_config_value(data_dir, "driver_mode", flag_mode.value)
    return flag_mode


async def _resolve_events_schema(
    flag_value: str | None,
    cfg: EdsConfig,
    data_dir: str,
) -> str:
    """Resolve events schema: flag vs config, persist to config.toml if changed."""
    if flag_value is None:
        return cfg.events_schema or "eds_events"
    if flag_value != cfg.events_schema:
        await set_config_value(data_dir, "events_schema", flag_value)
    return flag_value


# ── Session management ─────────────────────────────────────────────────────────

async def _start_session(
    api_url: str,
    api_key: str,
    server_id: str,
    data_dir: str,
) -> dict[str, Any]:
    """Start a new EDS session with Shopmonkey HQ and write the NATS credentials.

    Retries up to _MAX_STARTUP_FAILURES times on transient network/HTTP errors
    using exponential jitter (100 ms base + up to 500 ms × attempt).  A 409
    "already running" response waits _ALREADY_RUNNING_DELAY_S seconds and retries
    without consuming a failure slot.
    """
    headers = {
        "Authorization": f"Bearer {api_key}",
        "User-Agent": f"Shopmonkey EDS Server/{CURRENT}",
    }
    payload = {
        "serverId": server_id,
        "version": CURRENT,
        "os": sys.platform,
    }
    failures = 0
    d: dict[str, Any]
    while True:
        try:
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.post(
                    f"{api_url}/v3/eds/internal",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status == 409:
                        _log.warning(
                            "[server] Session already running — waiting %ds before retry...",
                            int(_ALREADY_RUNNING_DELAY_S),
                        )
                        await asyncio.sleep(_ALREADY_RUNNING_DELAY_S)
                        continue
                    resp.raise_for_status()
                    data = await resp.json()
                    if not data.get("success"):
                        raise click.ClickException(
                            f"Session start failed: {data.get('message', 'unknown')}"
                        )
                    d = data["data"]
        except click.ClickException:
            raise
        except (aiohttp.ClientResponseError, aiohttp.ClientError, asyncio.TimeoutError) as exc:
            failures += 1
            if failures >= _MAX_STARTUP_FAILURES:
                _log.error(
                    "[server] Session start failed after %d attempts: %s", failures, exc
                )
                raise
            delay = _STARTUP_RETRY_BASE_S + random.uniform(0, _STARTUP_RETRY_JITTER_S * failures)
            _log.warning(
                "[server] Session start failed (attempt %d/%d): %s — retrying in %.2fs",
                failures, _MAX_STARTUP_FAILURES, exc, delay,
            )
            await asyncio.sleep(delay)
            continue
        break

    session_id = d["sessionId"]
    creds_b64: str = d.get("credentials", "")
    nats_url: str = d.get("natsUrl", "nats://connect.nats.shopmonkey.pub")
    company_ids: list[str] = d.get("companyIds", ["*"])

    # Write credentials file
    creds_dir = Path(data_dir) / session_id
    creds_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    creds_file = creds_dir / "nats.creds"
    if creds_b64:
        import base64
        raw = base64.b64decode(creds_b64)
        fd = os.open(creds_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            os.write(fd, raw)
        finally:
            os.close(fd)

    return {
        "sessionId": session_id,
        "natsUrl": nats_url,
        "credentialsFile": str(creds_file) if creds_b64 else "",
        "companyIds": company_ids,
    }


async def _send_logs_to_hq(
    api_url: str,
    api_key: str,
    session_id: str,
    data_dir: str,
) -> tuple[bool, str | None]:
    """Upload the most-recent .log file from data_dir to Shopmonkey HQ for remote diagnostics.

    Flow (mirrors .NET SessionService.SendLogsAsync):
      1. POST /v3/eds/internal/{sessionId}/log  → pre-signed upload URL
      2. Gzip the log file to a temp path
      3. PUT the .gz to the pre-signed URL
    """
    try:
        log_files = sorted(
            Path(data_dir).glob("*.log"),
            key=lambda f: f.stat().st_mtime,
            reverse=True,
        )
        if not log_files:
            _log.debug("[sendlogs] No log files found in %s", data_dir)
            return True, None

        log_file = log_files[0]
        headers = {
            "Authorization": f"Bearer {api_key}",
            "User-Agent": f"Shopmonkey EDS Server/{CURRENT}",
        }

        # Step 1: get a pre-signed upload URL from HQ
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.post(
                f"{api_url}/v3/eds/internal/{session_id}/log",
                json={},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                resp.raise_for_status()
                body = await resp.json()
                upload_url: str = body["data"]["url"]

        # Step 2: gzip the log file to a temp path
        gz_path = Path(data_dir) / (log_file.name + ".gz")
        try:
            with log_file.open("rb") as src, gzip.open(gz_path, "wb") as dst:
                shutil.copyfileobj(src, dst)

            # Step 3: PUT to the pre-signed URL
            gz_bytes = gz_path.read_bytes()
            async with aiohttp.ClientSession() as session:
                async with session.put(
                    upload_url,
                    data=gz_bytes,
                    headers={"Content-Type": "application/x-tgz"},
                    timeout=aiohttp.ClientTimeout(total=120),
                ) as resp:
                    resp.raise_for_status()
        finally:
            gz_path.unlink(missing_ok=True)

        _log.info("[sendlogs] Uploaded %s to HQ.", log_file.name)
        return True, None

    except Exception as exc:
        _log.error("[sendlogs] Failed to upload logs to HQ: %s", exc)
        return False, str(exc)


async def _end_session(api_url: str, api_key: str, session_id: str) -> None:
    """Notify HQ that this session has ended."""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "User-Agent": f"Shopmonkey EDS Server/{CURRENT}",
    }
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            await session.delete(
                f"{api_url}/v3/eds/internal/{session_id}",
                timeout=aiohttp.ClientTimeout(total=10),
            )
    except Exception as exc:
        _log.debug("Session end notification failed (non-fatal): %s", exc)


def _platform_suffix() -> str:
    import platform
    system = platform.system().lower()
    machine = platform.machine().lower()
    if system == "darwin":
        return "osx-arm64" if "arm" in machine else "osx-x64"
    if system == "windows":
        return "win-x64"
    return "linux-x64"
