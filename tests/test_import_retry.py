"""Unit tests for the import retry mechanism.

Covers:
- ExportTimeoutError structure and inheritance
- poll_until_complete raising ExportTimeoutError (timeout and per-table failure)
- poll_download_with_retry happy path
- poll_download_with_retry partial download + retry on timeout
- poll_download_with_retry exhausting all retries
- Correct retry table targeting (only incomplete tables)
- Backoff delay values
- Filter param forwarding to retry jobs
- Short-circuit when all tables complete despite timeout flag
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eds.importer.importer import (
    ExportJobRequest,
    ExportJobResponse,
    ExportTimeoutError,
    TableExportData,
    TableExportInfo,
    _IMPORT_RETRY_DELAYS,
    poll_download_with_retry,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _completed_job(*tables: str) -> ExportJobResponse:
    """Fully completed job with download URLs for every named table."""
    return ExportJobResponse(
        completed=True,
        tables={
            t: TableExportData(
                status="Completed",
                urls=[f"https://export.example.com/{t}.ndjson.gz"],
            )
            for t in tables
        },
    )


def _partial_job(**table_states: bool) -> ExportJobResponse:
    """Partial job where True = has download URL, False = still pending."""
    return ExportJobResponse(
        completed=False,
        tables={
            name: TableExportData(
                status="Completed" if has_url else "Pending",
                urls=[f"https://export.example.com/{name}.ndjson.gz"] if has_url else [],
            )
            for name, has_url in table_states.items()
        },
    )


def _make_infos(*tables: str) -> list[TableExportInfo]:
    return [TableExportInfo(table=t, timestamp=datetime.now(timezone.utc)) for t in tables]


def _make_pairs(*tables: str) -> list[tuple[str, str]]:
    return [(t, f"/tmp/{t}.ndjson.gz") for t in tables]


# ── ExportTimeoutError ────────────────────────────────────────────────────────

def test_export_timeout_error_is_timeout_error() -> None:
    exc = ExportTimeoutError("timed out")
    assert isinstance(exc, TimeoutError)


def test_export_timeout_error_carries_partial_job() -> None:
    partial = _partial_job(orders=True)
    exc = ExportTimeoutError("timed out", partial_job=partial)
    assert exc.partial_job is partial
    assert str(exc) == "timed out"


def test_export_timeout_error_without_partial_job() -> None:
    exc = ExportTimeoutError("no data")
    assert exc.partial_job is None


# ── poll_until_complete raises ExportTimeoutError ─────────────────────────────

@pytest.mark.asyncio
async def test_poll_until_complete_raises_on_table_failure() -> None:
    """A failed table status triggers ExportTimeoutError with the partial job attached."""
    from eds.importer.importer import poll_until_complete

    failed_job = ExportJobResponse(
        completed=False,
        tables={
            "orders": TableExportData(status="Completed", urls=["https://x/orders.gz"]),
            "invoices": TableExportData(status="Failed", error="disk full"),
        },
    )

    with patch("eds.importer.importer._check_job", new=AsyncMock(return_value=failed_job)), \
         patch("asyncio.sleep", new=AsyncMock()):
        with pytest.raises(ExportTimeoutError) as exc_info:
            await poll_until_complete("https://api.example.com", "tok", "job-1")

    assert exc_info.value.partial_job is failed_job
    assert "invoices" in str(exc_info.value)


@pytest.mark.asyncio
async def test_poll_until_complete_raises_on_deadline() -> None:
    """Exceeding the deadline raises ExportTimeoutError with the last known job state."""
    from eds.importer.importer import poll_until_complete

    pending_job = ExportJobResponse(
        completed=False,
        tables={"orders": TableExportData(status="Pending")},
    )

    # Patch the timeout to -1 so deadline = now - 1, which is immediately in the past.
    with patch("eds.importer.importer._check_job", new=AsyncMock(return_value=pending_job)), \
         patch("asyncio.sleep", new=AsyncMock()), \
         patch("eds.importer.importer._POLL_TIMEOUT_SECONDS", new=-1):
        with pytest.raises(ExportTimeoutError) as exc_info:
            await poll_until_complete("https://api.example.com", "tok", "job-1")

    assert isinstance(exc_info.value, ExportTimeoutError)


# ── poll_download_with_retry: happy path ──────────────────────────────────────

@pytest.mark.asyncio
async def test_success_on_first_attempt() -> None:
    """No retry needed when the first poll completes all tables."""
    job = _completed_job("orders", "customers")
    infos = _make_infos("orders", "customers")
    pairs = _make_pairs("orders", "customers")

    with patch("eds.importer.importer.poll_until_complete", new=AsyncMock(return_value=job)), \
         patch("eds.importer.importer.bulk_download", new=AsyncMock(return_value=(infos, pairs))), \
         patch("eds.importer.importer.create_export_job", new=AsyncMock()) as mock_create:
        result_infos, result_pairs = await poll_download_with_retry(
            "https://api.example.com", "tok", "job-1", "/tmp"
        )

    assert result_infos == infos
    assert result_pairs == pairs
    mock_create.assert_not_called()


# ── poll_download_with_retry: retry after partial timeout ─────────────────────

@pytest.mark.asyncio
async def test_partial_download_then_retry_succeeds() -> None:
    """Completed tables are downloaded immediately; incomplete tables trigger a retry."""
    partial = _partial_job(orders=True, customers=False)
    retry_job = _completed_job("customers")

    partial_infos = _make_infos("orders")
    partial_pairs = _make_pairs("orders")
    retry_infos = _make_infos("customers")
    retry_pairs = _make_pairs("customers")

    poll_count = 0

    async def mock_poll(_api, _key, _job_id):
        nonlocal poll_count
        poll_count += 1
        if poll_count == 1:
            raise ExportTimeoutError("timed out", partial_job=partial)
        return retry_job

    download_count = 0

    async def mock_download(job, dest_dir, checkpoint=None):
        nonlocal download_count
        download_count += 1
        return (partial_infos, partial_pairs) if download_count == 1 else (retry_infos, retry_pairs)

    with patch("eds.importer.importer.poll_until_complete", new=mock_poll), \
         patch("eds.importer.importer.bulk_download", new=mock_download), \
         patch("eds.importer.importer.create_export_job", new=AsyncMock(return_value="job-2")), \
         patch("asyncio.sleep", new=AsyncMock()):
        result_infos, result_pairs = await poll_download_with_retry(
            "https://api.example.com", "tok", "job-1", "/tmp"
        )

    assert len(result_infos) == 2
    assert len(result_pairs) == 2
    assert poll_count == 2
    assert download_count == 2


@pytest.mark.asyncio
async def test_all_tables_timeout_retry_succeeds() -> None:
    """When no tables have URLs yet, all are queued for retry."""
    partial = _partial_job(orders=False, customers=False)
    retry_job = _completed_job("orders", "customers")

    infos = _make_infos("orders", "customers")
    pairs = _make_pairs("orders", "customers")

    poll_count = 0

    async def mock_poll(_api, _key, _job_id):
        nonlocal poll_count
        poll_count += 1
        if poll_count == 1:
            raise ExportTimeoutError("timed out", partial_job=partial)
        return retry_job

    with patch("eds.importer.importer.poll_until_complete", new=mock_poll), \
         patch("eds.importer.importer.bulk_download", new=AsyncMock(return_value=(infos, pairs))), \
         patch("eds.importer.importer.create_export_job", new=AsyncMock(return_value="job-2")), \
         patch("asyncio.sleep", new=AsyncMock()):
        result_infos, result_pairs = await poll_download_with_retry(
            "https://api.example.com", "tok", "job-1", "/tmp"
        )

    assert result_infos == infos


# ── poll_download_with_retry: retry targeting ─────────────────────────────────

@pytest.mark.asyncio
async def test_retry_requests_only_incomplete_tables() -> None:
    """The new export job is scoped to only the tables that did not receive URLs."""
    partial = ExportJobResponse(
        completed=False,
        tables={
            "orders":    TableExportData(status="Completed", urls=["https://x/orders.gz"]),
            "customers": TableExportData(status="Pending",   urls=[]),
            "invoices":  TableExportData(status="Failed",    urls=[]),
        },
    )

    created_requests: list[ExportJobRequest] = []

    async def mock_create(_api, _key, request):
        created_requests.append(request)
        return "job-retry"

    poll_count = 0

    async def mock_poll(_api, _key, _job_id):
        nonlocal poll_count
        poll_count += 1
        if poll_count == 1:
            raise ExportTimeoutError("timeout", partial_job=partial)
        return _completed_job("customers", "invoices")

    with patch("eds.importer.importer.poll_until_complete", new=mock_poll), \
         patch("eds.importer.importer.bulk_download", new=AsyncMock(return_value=([], []))), \
         patch("eds.importer.importer.create_export_job", new=mock_create), \
         patch("asyncio.sleep", new=AsyncMock()):
        await poll_download_with_retry("https://api.example.com", "tok", "job-1", "/tmp")

    assert len(created_requests) == 1
    assert set(created_requests[0].tables) == {"customers", "invoices"}


@pytest.mark.asyncio
async def test_filter_params_forwarded_to_retry_jobs() -> None:
    """company_ids and location_ids are passed through to all retry export jobs."""
    partial = _partial_job(orders=False)

    created_requests: list[ExportJobRequest] = []

    async def mock_create(_api, _key, request):
        created_requests.append(request)
        return f"job-{len(created_requests)}"

    poll_count = 0

    async def mock_poll(_api, _key, _job_id):
        nonlocal poll_count
        poll_count += 1
        if poll_count == 1:
            raise ExportTimeoutError("timeout", partial_job=partial)
        return _completed_job("orders")

    with patch("eds.importer.importer.poll_until_complete", new=mock_poll), \
         patch("eds.importer.importer.bulk_download", new=AsyncMock(return_value=([], []))), \
         patch("eds.importer.importer.create_export_job", new=mock_create), \
         patch("asyncio.sleep", new=AsyncMock()):
        await poll_download_with_retry(
            "https://api.example.com", "tok", "job-0", "/tmp",
            company_ids=["c1", "c2"],
            location_ids=["l1"],
        )

    assert len(created_requests) == 1
    assert created_requests[0].company_ids == ["c1", "c2"]
    assert created_requests[0].location_ids == ["l1"]


# ── poll_download_with_retry: all-complete short-circuit ──────────────────────

@pytest.mark.asyncio
async def test_no_retry_when_all_tables_have_urls_despite_timeout() -> None:
    """If every table has URLs when the timeout fires, skip retry entirely."""
    all_done = _partial_job(orders=True, customers=True)

    infos = _make_infos("orders", "customers")
    pairs = _make_pairs("orders", "customers")

    create_called = False

    async def mock_create(_api, _key, _request):
        nonlocal create_called
        create_called = True
        return "should-not-be-reached"

    with patch("eds.importer.importer.poll_until_complete",
               new=AsyncMock(side_effect=ExportTimeoutError("timeout", partial_job=all_done))), \
         patch("eds.importer.importer.bulk_download", new=AsyncMock(return_value=(infos, pairs))), \
         patch("eds.importer.importer.create_export_job", new=mock_create):
        result_infos, result_pairs = await poll_download_with_retry(
            "https://api.example.com", "tok", "job-1", "/tmp"
        )

    assert not create_called
    assert result_infos == infos


# ── poll_download_with_retry: retry exhaustion ────────────────────────────────

@pytest.mark.asyncio
async def test_raises_after_max_retries_exceeded() -> None:
    """ExportTimeoutError is raised after all retries are consumed."""
    partial = _partial_job(orders=False)

    with patch("eds.importer.importer.poll_until_complete",
               new=AsyncMock(side_effect=ExportTimeoutError("timeout", partial_job=partial))), \
         patch("eds.importer.importer.bulk_download", new=AsyncMock(return_value=([], []))), \
         patch("eds.importer.importer.create_export_job", new=AsyncMock(return_value="j")), \
         patch("asyncio.sleep", new=AsyncMock()):
        with pytest.raises(ExportTimeoutError) as exc_info:
            await poll_download_with_retry(
                "https://api.example.com", "tok", "job-1", "/tmp",
                max_retries=2,
            )

    assert "orders" in str(exc_info.value)
    assert exc_info.value.partial_job is not None


@pytest.mark.asyncio
async def test_correct_total_poll_attempts() -> None:
    """Exactly max_retries + 1 poll attempts are made before giving up."""
    partial = _partial_job(orders=False)
    poll_calls = 0

    async def mock_poll(_api, _key, _job_id):
        nonlocal poll_calls
        poll_calls += 1
        raise ExportTimeoutError("timeout", partial_job=partial)

    with patch("eds.importer.importer.poll_until_complete", new=mock_poll), \
         patch("eds.importer.importer.bulk_download", new=AsyncMock(return_value=([], []))), \
         patch("eds.importer.importer.create_export_job", new=AsyncMock(return_value="j")), \
         patch("asyncio.sleep", new=AsyncMock()):
        with pytest.raises(ExportTimeoutError):
            await poll_download_with_retry(
                "https://api.example.com", "tok", "job-1", "/tmp",
                max_retries=4,
            )

    assert poll_calls == 5  # 1 initial + 4 retries


# ── poll_download_with_retry: backoff ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_backoff_delays_match_retry_schedule() -> None:
    """asyncio.sleep is called with the values from _IMPORT_RETRY_DELAYS."""
    partial = _partial_job(t=False)
    sleep_calls: list[float] = []

    async def mock_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    with patch("eds.importer.importer.poll_until_complete",
               new=AsyncMock(side_effect=ExportTimeoutError("timeout", partial_job=partial))), \
         patch("eds.importer.importer.bulk_download", new=AsyncMock(return_value=([], []))), \
         patch("eds.importer.importer.create_export_job", new=AsyncMock(return_value="j")), \
         patch("asyncio.sleep", new=mock_sleep):
        with pytest.raises(ExportTimeoutError):
            await poll_download_with_retry(
                "https://api.example.com", "tok", "job-1", "/tmp",
                max_retries=3,
            )

    # Attempts 1, 2, 3 each sleep before their poll; attempt 0 does not.
    assert sleep_calls == _IMPORT_RETRY_DELAYS[:3]


@pytest.mark.asyncio
async def test_backoff_caps_at_last_delay_value() -> None:
    """When retries exceed the delay table length, the last delay is reused."""
    partial = _partial_job(t=False)
    sleep_calls: list[float] = []

    async def mock_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    with patch("eds.importer.importer.poll_until_complete",
               new=AsyncMock(side_effect=ExportTimeoutError("timeout", partial_job=partial))), \
         patch("eds.importer.importer.bulk_download", new=AsyncMock(return_value=([], []))), \
         patch("eds.importer.importer.create_export_job", new=AsyncMock(return_value="j")), \
         patch("asyncio.sleep", new=mock_sleep):
        with pytest.raises(ExportTimeoutError):
            # 7 retries — more than the 5-entry delay table.
            await poll_download_with_retry(
                "https://api.example.com", "tok", "job-1", "/tmp",
                max_retries=7,
            )

    # Delays beyond the table length should reuse the last entry (480).
    assert sleep_calls[4] == _IMPORT_RETRY_DELAYS[-1]  # 5th retry
    assert sleep_calls[5] == _IMPORT_RETRY_DELAYS[-1]  # 6th retry
    assert sleep_calls[6] == _IMPORT_RETRY_DELAYS[-1]  # 7th retry


# ── Accumulated results across sub-jobs ───────────────────────────────────────

@pytest.mark.asyncio
async def test_results_accumulated_across_multiple_retries() -> None:
    """Table infos and file pairs from all sub-jobs are merged in the return value."""
    partial1 = _partial_job(orders=True, customers=False, invoices=False)
    partial2 = _partial_job(customers=True, invoices=False)
    final_job = _completed_job("invoices")

    infos_1 = _make_infos("orders")
    pairs_1 = _make_pairs("orders")
    infos_2 = _make_infos("customers")
    pairs_2 = _make_pairs("customers")
    infos_3 = _make_infos("invoices")
    pairs_3 = _make_pairs("invoices")

    poll_count = 0

    async def mock_poll(_api, _key, _job_id):
        nonlocal poll_count
        poll_count += 1
        if poll_count == 1:
            raise ExportTimeoutError("timeout1", partial_job=partial1)
        if poll_count == 2:
            raise ExportTimeoutError("timeout2", partial_job=partial2)
        return final_job

    download_count = 0

    async def mock_download(job, dest_dir, checkpoint=None):
        nonlocal download_count
        download_count += 1
        return [
            (infos_1, pairs_1),
            (infos_2, pairs_2),
            (infos_3, pairs_3),
        ][download_count - 1]

    with patch("eds.importer.importer.poll_until_complete", new=mock_poll), \
         patch("eds.importer.importer.bulk_download", new=mock_download), \
         patch("eds.importer.importer.create_export_job", new=AsyncMock(return_value="j")), \
         patch("asyncio.sleep", new=AsyncMock()):
        result_infos, result_pairs = await poll_download_with_retry(
            "https://api.example.com", "tok", "job-1", "/tmp"
        )

    assert len(result_infos) == 3
    assert len(result_pairs) == 3
    tables_returned = {i.table for i in result_infos}
    assert tables_returned == {"orders", "customers", "invoices"}
