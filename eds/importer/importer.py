"""Bulk import pipeline — mirrors cmd/import.go and EDS.Cli/ImportService.cs.

Flow:
  1. CreateExportJob  → Shopmonkey HQ kicks off a CRDB export
  2. PollUntilComplete → polls until all tables are exported
  3. BulkDownload      → downloads all .ndjson.gz files (10 parallel)
  4. Import            → each driver processes files via process()/flush()
"""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote as url_quote, urlparse

import aiofiles
import aiohttp

from eds.core.driver import Driver
from eds.core.models import DbChangeEvent
from eds.core.retry import execute as retry, is_transient
from eds.core.tracker import Tracker

_log = logging.getLogger(__name__)

_CHECKPOINT_KEY = "import:checkpoint"
_TABLE_EXPORT_KEY = "import:table-export"


# ── API types ──────────────────────────────────────────────────────────────────

@dataclass
class ExportJobRequest:
    tables: list[str] | None = None
    company_ids: list[str] | None = None
    location_ids: list[str] | None = None


@dataclass
class TableExportData:
    status: str = ""
    error: str = ""
    urls: list[str] = field(default_factory=list)
    cursor: str = ""


@dataclass
class ExportJobResponse:
    job_id: str = ""
    completed: bool = False
    tables: dict[str, TableExportData] = field(default_factory=dict)

    def progress_string(self) -> str:
        done = sum(1 for t in self.tables.values() if t.status == "Completed")
        total = len(self.tables)
        pct = 100.0 * done / total if total else 0
        return f"{done}/{total} ({pct:.1f}%)"


@dataclass
class ImportCheckpoint:
    job_id: str = ""
    download_dir: str = ""
    completed_files: list[str] = field(default_factory=list)
    started_at: str = ""


@dataclass
class TableExportInfo:
    table: str
    timestamp: datetime


# ── HTTP helpers ───────────────────────────────────────────────────────────────

def _make_headers(api_key: str) -> dict[str, str]:
    from eds.version import CURRENT
    return {
        "Authorization": f"Bearer {api_key}",
        "User-Agent": f"Shopmonkey EDS Server/{CURRENT}",
    }


async def create_export_job(
    api_url: str,
    api_key: str,
    request: ExportJobRequest,
) -> str:
    body: dict[str, Any] = {}
    if request.tables:
        body["tables"] = request.tables
    if request.company_ids:
        body["companyIds"] = request.company_ids
    if request.location_ids:
        body["locationIds"] = request.location_ids

    async def _post() -> str:
        async with aiohttp.ClientSession(headers=_make_headers(api_key)) as session:
            async with session.post(
                f"{api_url}/v3/export/bulk",
                json=body,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
                if not data.get("success"):
                    raise RuntimeError(f"API error: {data.get('message', 'unknown')}")
                return data["data"]["jobId"]

    return await retry(_post, operation_name="create export job")


_POLL_TIMEOUT_SECONDS = 4 * 60 * 60  # 4 hours — export jobs should never take this long

# Backoff delays (seconds) between successive import retries — doubles each time up to 8 min.
_IMPORT_RETRY_DELAYS = [30, 60, 120, 240, 480]


class ExportTimeoutError(TimeoutError):
    """Raised when an export job times out or one or more tables fail server-side.

    *partial_job* holds the last known ``ExportJobResponse``, allowing the caller
    to identify which tables completed (have ``urls``) and which still need retrying.
    """

    def __init__(self, message: str, partial_job: ExportJobResponse | None = None) -> None:
        super().__init__(message)
        self.partial_job = partial_job


async def poll_until_complete(
    api_url: str,
    api_key: str,
    job_id: str,
) -> ExportJobResponse:
    last_logged = datetime.min
    deadline = datetime.now(timezone.utc).timestamp() + _POLL_TIMEOUT_SECONDS
    last_job: ExportJobResponse | None = None

    while True:
        if datetime.now(timezone.utc).timestamp() > deadline:
            raise ExportTimeoutError(
                f"Export job {job_id!r} did not complete within "
                f"{_POLL_TIMEOUT_SECONDS // 3600} hours.",
                partial_job=last_job,
            )
        now = datetime.now(timezone.utc)
        if (now - last_logged.replace(tzinfo=timezone.utc)).total_seconds() > 60:
            _log.info("[import] Checking export status (%s)", job_id)
            last_logged = now

        try:
            job = await _check_job(api_url, api_key, job_id)
        except Exception as exc:
            if is_transient(exc):
                _log.warning("[import] Transient error polling export — retrying in 15s: %s", exc)
                await asyncio.sleep(15)
                last_logged = datetime.min
                continue
            raise

        last_job = job

        failed = [name for name, data in job.tables.items() if data.status == "Failed"]
        if failed:
            raise ExportTimeoutError(
                f"Export of {len(failed)} table(s) failed server-side: {failed}",
                partial_job=job,
            )

        _log.debug("[import] Export status: %s", job.progress_string())
        if job.completed:
            _log.info("[import] Export complete: %s", job.progress_string())
            return job

        await asyncio.sleep(5)


async def poll_download_with_retry(
    api_url: str,
    api_key: str,
    initial_job_id: str,
    dest_dir: str,
    checkpoint: ImportCheckpoint | None = None,
    company_ids: list[str] | None = None,
    location_ids: list[str] | None = None,
    max_retries: int = 5,
) -> tuple[list[TableExportInfo], list[tuple[str, Path]]]:
    """Poll *initial_job_id* then download results, retrying incomplete tables on failure.

    When the server times out or individual tables fail, any tables that have already
    received download URLs are fetched immediately.  A fresh targeted export job is then
    created for the remaining tables and polled again, up to *max_retries* times with
    exponential back-off (30 s → 60 s → 120 s → 240 s → 480 s).

    Returns the accumulated ``(table_infos, file_table_pairs)`` across all sub-jobs.
    """
    all_table_infos: list[TableExportInfo] = []
    all_file_table_pairs: list[tuple[str, Path]] = []

    current_job_id = initial_job_id
    # None means "all tables from this job"; populated after the first timeout.
    pending_tables: list[str] | None = None

    for attempt in range(max_retries + 1):
        if attempt > 0:
            delay = _IMPORT_RETRY_DELAYS[min(attempt - 1, len(_IMPORT_RETRY_DELAYS) - 1)]
            _log.warning(
                "[import] Retry %d/%d — %d table(s) still pending, backing off %ds: %s",
                attempt, max_retries, len(pending_tables or []), delay, pending_tables,
            )
            await asyncio.sleep(delay)

            # Start a new targeted export for just the tables that didn't finish.
            sub_request = ExportJobRequest(
                tables=pending_tables,
                company_ids=company_ids,
                location_ids=location_ids,
            )
            current_job_id = await create_export_job(api_url, api_key, sub_request)
            _log.info(
                "[import] Retry export job created: %s (tables: %s)",
                current_job_id, pending_tables,
            )

        try:
            job = await poll_until_complete(api_url, api_key, current_job_id)
            # All tables in this sub-job succeeded.
            ti, fp = await bulk_download(job, dest_dir, checkpoint)
            all_table_infos.extend(ti)
            all_file_table_pairs.extend(fp)
            _log.info(
                "[import] Sub-job complete — %d table(s) downloaded (attempt %d/%d).",
                len(ti), attempt + 1, max_retries + 1,
            )
            return all_table_infos, all_file_table_pairs

        except ExportTimeoutError as exc:
            partial = exc.partial_job

            # Separate completed tables (have URLs) from those that still need retrying.
            completed_data: dict[str, TableExportData] = {}
            still_pending: list[str] = []
            if partial:
                for name, data in partial.tables.items():
                    if data.urls:
                        completed_data[name] = data
                    else:
                        still_pending.append(name)
            elif pending_tables:
                still_pending = list(pending_tables)

            # Download whatever completed before the timeout/failure.
            if completed_data:
                partial_response = ExportJobResponse(completed=False, tables=completed_data)
                ti, fp = await bulk_download(partial_response, dest_dir, checkpoint)
                all_table_infos.extend(ti)
                all_file_table_pairs.extend(fp)
                _log.info(
                    "[import] Downloaded %d table(s) from partial job before timeout.",
                    len(ti),
                )

            _log.warning(
                "[import] %d table(s) did not complete on attempt %d/%d: %s",
                len(still_pending), attempt + 1, max_retries + 1, still_pending,
            )

            if not still_pending:
                # Everything with URLs was downloaded — treat as success.
                return all_table_infos, all_file_table_pairs

            pending_tables = still_pending

            if attempt == max_retries:
                raise ExportTimeoutError(
                    f"Import failed after {max_retries} retries. "
                    f"{len(pending_tables)} table(s) never completed: {pending_tables}",
                    partial_job=partial,
                ) from exc

    # Unreachable, but satisfies the type checker.
    return all_table_infos, all_file_table_pairs


async def _check_job(api_url: str, api_key: str, job_id: str) -> ExportJobResponse:
    safe_id = url_quote(job_id, safe="")
    async with aiohttp.ClientSession(headers=_make_headers(api_key)) as session:
        async with session.get(
            f"{api_url}/v3/export/bulk/{safe_id}",
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
            if not data.get("success"):
                raise RuntimeError(f"API error: {data.get('message', 'unknown')}")
            raw = data["data"]
            tables = {
                name: TableExportData(
                    status=td.get("status", ""),
                    error=td.get("error", ""),
                    urls=td.get("urls", []),
                    cursor=td.get("cursor", ""),
                )
                for name, td in raw.get("tables", {}).items()
            }
            return ExportJobResponse(
                completed=raw.get("completed", False),
                tables=tables,
            )


# ── Download ───────────────────────────────────────────────────────────────────

async def bulk_download(
    job: ExportJobResponse,
    dest_dir: str,
    checkpoint: ImportCheckpoint | None = None,
) -> tuple[list[TableExportInfo], list[tuple[str, Path]]]:
    """Download all export files into dest_dir with up to 10 concurrent downloads.

    Returns:
        (table_export_infos, file_table_pairs) where file_table_pairs is a list of
        (table_name, local_path) tuples suitable for direct-import drivers.
    """
    os.makedirs(dest_dir, exist_ok=True)
    completed = set(checkpoint.completed_files if checkpoint else [])

    table_ts: dict[str, datetime] = {}
    downloads: list[tuple[str, str]] = []  # (url, table_name)

    for table, data in job.tables.items():
        if not data.urls:
            ns = int(data.cursor) if data.cursor.isdigit() else 0
            table_ts[table] = datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc)
            continue
        latest = datetime.min.replace(tzinfo=timezone.utc)
        for url in data.urls:
            filename = Path(url.split("?")[0]).name
            ts = _parse_timestamp_from_filename(filename)
            if ts and ts > latest:
                latest = ts
            downloads.append((url, table))
        table_ts[table] = latest if latest > datetime.min.replace(tzinfo=timezone.utc) else datetime.now(timezone.utc)

    sem = asyncio.Semaphore(10)
    total_bytes = 0
    done_count = 0
    # Track (table, dest_path) for each completed download
    file_table_pairs: list[tuple[str, Path]] = []
    pairs_lock = asyncio.Lock()

    async def _download_one(url: str, table: str) -> None:
        nonlocal total_bytes, done_count
        # Enforce HTTPS-only to prevent SSRF via file:// or other schemes
        if urlparse(url).scheme not in ("https", "http"):
            raise ValueError(f"Unsafe download URL scheme: {url}")
        filename = Path(url.split("?")[0]).name
        dest = Path(dest_dir) / filename
        if str(dest) in completed:
            async with pairs_lock:
                file_table_pairs.append((table, dest))
            return

        # Path containment guard
        resolved = str(dest.resolve())
        if not resolved.startswith(str(Path(dest_dir).resolve()) + os.sep):
            raise ValueError(f"Download path escaped destination: {url}")

        async with sem:
            async def _fetch() -> bytes:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=600)) as r:
                        r.raise_for_status()
                        return await r.read()

            data = await retry(_fetch, operation_name=f"download {filename}")
            async with aiofiles.open(dest, "wb") as f:
                await f.write(data)
            total_bytes += len(data)
            done_count += 1
            _log.debug("[import] Downloaded %d/%d", done_count, len(downloads))

        async with pairs_lock:
            file_table_pairs.append((table, dest))

    await asyncio.gather(*(_download_one(url, table) for url, table in downloads))
    _log.info("[import] Downloaded %d file(s) (%d bytes)", len(downloads), total_bytes)

    return [TableExportInfo(table=t, timestamp=ts) for t, ts in table_ts.items()], file_table_pairs


def _parse_timestamp_from_filename(filename: str) -> datetime | None:
    """Extract the Unix-nanosecond timestamp embedded in CRDB export filenames."""
    try:
        # Typical format: data-<timestamp>-<index>-<hash>.ndjson.gz
        parts = filename.split("-")
        if len(parts) >= 2:
            ns = int(parts[1])
            return datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc)
    except (ValueError, IndexError):
        pass
    return None


# ── Import (driver-side) ───────────────────────────────────────────────────────

async def import_files(
    driver: Driver,
    files: list[Path],
    parallel: int = 4,
    dry_run: bool = False,
    tracker: Tracker | None = None,
    checkpoint: ImportCheckpoint | None = None,
) -> None:
    """Feed .ndjson.gz export files through the driver one record at a time.

    *parallel* controls the maximum number of files processed concurrently.
    A semaphore limits concurrency so that memory pressure stays bounded even
    when hundreds of export files are present.
    """
    completed = set(checkpoint.completed_files if checkpoint else [])
    sem = asyncio.Semaphore(max(1, parallel))

    async def _process_file(path: Path) -> None:
        async with sem:
            if str(path) in completed:
                _log.debug("[import] Skipping already-completed file: %s", path.name)
                return

            _log.info("[import] Processing %s", path.name)
            count = 0
            opener = gzip.open if path.suffix == ".gz" else open
            with opener(path, "rb") as fh:  # type: ignore[call-overload]
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        raw = json.loads(line)
                    except json.JSONDecodeError as exc:
                        _log.warning("[import] Skipping invalid JSON line in %s: %s", path.name, exc)
                        continue

                    evt = DbChangeEvent.from_dict(raw)
                    evt.imported = True
                    if not dry_run:
                        await driver.process(evt)
                    count += 1

            if not dry_run:
                await driver.flush()

            _log.info("[import] %s: %d records", path.name, count)

            if tracker and checkpoint:
                checkpoint.completed_files.append(str(path))
                await tracker.set_key(_CHECKPOINT_KEY, json.dumps({
                    "jobId": checkpoint.job_id,
                    "downloadDir": checkpoint.download_dir,
                    "completedFiles": checkpoint.completed_files,
                    "startedAt": checkpoint.started_at,
                }))

    await asyncio.gather(*(_process_file(p) for p in files))


async def import_files_direct(
    driver: Driver,
    file_table_pairs: list[tuple[str, Path]],
) -> None:
    """Pass raw .ndjson.gz files directly to a driver that supports direct import.
    Skips row-by-row parsing — the driver handles the files natively (e.g. S3 upload, file copy).
    """
    _log.info("[import] Using direct import for %d file(s)", len(file_table_pairs))
    await driver.direct_import(file_table_pairs)
    _log.info("[import] Direct import complete")


# ── Checkpoint helpers ─────────────────────────────────────────────────────────

async def save_checkpoint(tracker: Tracker, cp: ImportCheckpoint) -> None:
    await tracker.set_key(_CHECKPOINT_KEY, json.dumps({
        "jobId": cp.job_id,
        "downloadDir": cp.download_dir,
        "completedFiles": cp.completed_files,
        "startedAt": cp.started_at,
    }))


async def load_checkpoint(tracker: Tracker) -> ImportCheckpoint | None:
    raw = await tracker.get_key(_CHECKPOINT_KEY)
    if raw is None:
        return None
    try:
        d = json.loads(raw)
        if not isinstance(d, dict):
            raise ValueError("checkpoint is not a JSON object")
        return ImportCheckpoint(
            job_id=d.get("jobId", ""),
            download_dir=d.get("downloadDir", ""),
            completed_files=d.get("completedFiles", []),
            started_at=d.get("startedAt", ""),
        )
    except Exception as exc:
        _log.warning("[import] Discarding unreadable checkpoint (%s) — starting fresh.", exc)
        await tracker.delete_keys(_CHECKPOINT_KEY)
        return None


async def save_table_export_info(tracker: Tracker, info: list[TableExportInfo]) -> None:
    data = [{"table": i.table, "timestamp": i.timestamp.isoformat()} for i in info]
    await tracker.set_key(_TABLE_EXPORT_KEY, json.dumps(data))


async def load_table_export_info(tracker: Tracker) -> list[TableExportInfo] | None:
    raw = await tracker.get_key(_TABLE_EXPORT_KEY)
    if raw is None:
        return None
    return [
        TableExportInfo(
            table=d["table"],
            timestamp=datetime.fromisoformat(d["timestamp"]),
        )
        for d in json.loads(raw)
    ]
