"""eds import — bulk data import from Shopmonkey HQ."""

from __future__ import annotations

import asyncio
import logging
import tempfile
from pathlib import Path

import click

from eds.infrastructure.config import load_config
from eds.infrastructure.tracker import SqliteTracker
from eds.infrastructure.schema_registry import SchemaRegistry
from eds.core.driver import new_driver, DriverConfig
from eds.importer.importer import (
    ExportJobRequest,
    ImportCheckpoint,
    create_export_job,
    poll_download_with_retry,
    import_files,
    load_checkpoint,
    save_checkpoint,
    save_table_export_info,
    import_files_direct,
)

import eds.drivers  # noqa: F401

_log = logging.getLogger(__name__)


@click.command()
@click.option("--url", envvar="EDS_URL", default="", help="Driver connection URL.")
@click.option("--api-key", envvar="EDS_TOKEN", default="", help="Shopmonkey API key.")
@click.option("--api-url", default="https://api.shopmonkey.cloud", hidden=True)
@click.option("--only", default="", help="Comma-separated list of tables to import.")
@click.option("--company-ids", default="", help="Filter by company IDs (comma-separated).")
@click.option("--location-ids", default="", help="Filter by location IDs (comma-separated).")
@click.option("--job-id", default="", help="Reuse an existing export job ID.")
@click.option("--dir", "import_dir", default="", help="Path to already-downloaded export files.")
@click.option("--parallel", default=4, show_default=True, help="Max parallel table workers.")
@click.option("--dry-run", is_flag=True, help="Parse without writing any rows.")
@click.option("--no-confirm", is_flag=True, help="Skip the delete-confirmation prompt.")
@click.option("--no-delete", is_flag=True, help="Insert rows only; do not drop/recreate tables.")
@click.option("--schema-only", is_flag=True, help="Create tables without importing rows.")
@click.option("--no-cleanup", is_flag=True, help="Keep the temporary download directory.")
@click.option("--resume", is_flag=True,
              help="Resume the last interrupted import (implies --no-delete --no-cleanup).")
@click.option("--driver-mode", "driver_mode", default=None,
              type=click.Choice(["upsert", "timeseries"], case_sensitive=False),
              help="Event writing mode: upsert (default) or timeseries.")
@click.option("--events-schema", "events_schema", default=None,
              help="Schema name for time-series events tables (default: eds_events).")
@click.pass_context
def import_cmd(
    ctx: click.Context,
    url: str,
    api_key: str,
    api_url: str,
    only: str,
    company_ids: str,
    location_ids: str,
    job_id: str,
    import_dir: str,
    parallel: int,
    dry_run: bool,
    no_confirm: bool,
    no_delete: bool,
    schema_only: bool,
    no_cleanup: bool,
    resume: bool,
    driver_mode: str | None,
    events_schema: str | None,
) -> None:
    """Run a one-time bulk data import then (optionally) start the server."""
    data_dir: str = ctx.obj["data_dir"]
    if resume:
        no_delete = True
        no_cleanup = True

    asyncio.run(_import(
        data_dir=data_dir,
        url=url,
        api_key=api_key,
        api_url=api_url,
        only=only,
        company_ids=company_ids,
        location_ids=location_ids,
        job_id=job_id,
        import_dir=import_dir,
        parallel=parallel,
        dry_run=dry_run,
        no_confirm=no_confirm,
        no_delete=no_delete,
        schema_only=schema_only,
        no_cleanup=no_cleanup,
        resume=resume,
        driver_mode_flag=driver_mode,
        events_schema_flag=events_schema,
    ))


async def _import(
    data_dir: str,
    url: str,
    api_key: str,
    api_url: str,
    only: str,
    company_ids: str,
    location_ids: str,
    job_id: str,
    import_dir: str,
    parallel: int = 4,
    dry_run: bool = False,
    no_confirm: bool = False,
    no_delete: bool = False,
    schema_only: bool = False,
    no_cleanup: bool = False,
    resume: bool = False,
    driver_mode_flag: str | None = None,
    events_schema_flag: str | None = None,
) -> None:
    cfg = load_config(data_dir)
    url = url or cfg.url
    api_key = api_key or cfg.token
    api_url = api_url or cfg.api_url

    if not url:
        raise click.ClickException("Driver URL required (--url or url in config.toml)")
    if not api_key:
        raise click.ClickException("API key required (--api-key or token in config.toml)")

    # Resolve driver mode (flag vs config, persist if changed, prompt on conflict)
    from eds.cli.server import _resolve_driver_mode, _resolve_events_schema
    mode = await _resolve_driver_mode(driver_mode_flag, cfg, data_dir, no_confirm=no_confirm)
    events_schema = await _resolve_events_schema(events_schema_flag, cfg, data_dir)

    tracker = SqliteTracker(Path(data_dir) / "state.db")
    await tracker.open()

    try:
        registry = SchemaRegistry(api_url=api_url, api_key=api_key, tracker=tracker)
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

        # Determine download directory
        checkpoint: ImportCheckpoint | None = None
        if resume:
            checkpoint = await load_checkpoint(tracker)
            if checkpoint:
                # Recover job_id from checkpoint so we re-poll the same export
                # job instead of creating a new one (handles crash during polling).
                if not job_id:
                    job_id = checkpoint.job_id
                import_dir = checkpoint.download_dir
                # Directory may be missing if the process was killed before
                # downloading started — recreate it so poll → download can proceed.
                Path(import_dir).mkdir(parents=True, exist_ok=True)
                _log.info("[import] Resuming job %s from %s (%d files completed)",
                          job_id, import_dir, len(checkpoint.completed_files))

        cleanup_dir = False
        if not import_dir:
            import_dir = tempfile.mkdtemp(prefix="eds-import-")
            cleanup_dir = not no_cleanup

        # Resolve filter lists once — used for both the initial job and any retries.
        tables = [t.strip() for t in only.split(",") if t.strip()] if only else None
        cids   = [c.strip() for c in company_ids.split(",")   if c.strip()] if company_ids   else None
        lids   = [l.strip() for l in location_ids.split(",")  if l.strip()] if location_ids  else None

        # Export job
        if not job_id and not list(Path(import_dir).glob("*.ndjson*")):
            request = ExportJobRequest(tables=tables, company_ids=cids, location_ids=lids)
            job_id = await create_export_job(api_url, api_key, request)
            _log.info("[import] Created export job: %s", job_id)

        if not checkpoint:
            checkpoint = ImportCheckpoint(
                job_id=job_id,
                download_dir=import_dir,
                started_at=__import__("datetime").datetime.utcnow().isoformat(),
            )
            await save_checkpoint(tracker, checkpoint)

        # Poll, download, and retry incomplete tables automatically on server-side timeout.
        file_table_pairs: list = []
        if job_id:
            table_infos, file_table_pairs = await poll_download_with_retry(
                api_url, api_key, job_id, import_dir, checkpoint,
                company_ids=cids, location_ids=lids,
            )
            await save_table_export_info(tracker, table_infos)

        if not dry_run and not no_confirm and not no_delete:
            click.confirm(
                "[import] This will drop and recreate all tables. Continue?",
                abort=True,
            )

        if schema_only:
            _log.info("[import] --schema-only: skipping row import")
            return

        # Find files to import
        files = sorted(Path(import_dir).glob("*.ndjson*"))
        if not files:
            _log.warning("[import] No .ndjson files found in %s", import_dir)
            return

        # Gap 5: On a fresh (non-resume) import, drop tables that are in the DB but
        # no longer in the current HQ schema so the DB ends up fully in sync.
        if not dry_run and not resume and not no_delete and file_table_pairs:
            known_tables = {table for table, _ in file_table_pairs}
            try:
                await driver.drop_orphan_tables(known_tables)
            except Exception as exc:  # noqa: BLE001
                _log.debug("[import] Orphan table cleanup skipped: %s", exc)

        # Reconcile DB schema before importing rows: add any columns that appeared
        # in the HQ schema since the tables were last created or migrated.  This
        # handles resume imports where the HQ schema has grown new columns that the
        # destination table doesn't have yet, preventing "Unknown column" errors.
        if not dry_run and driver.supports_migration() and file_table_pairs:
            unique_tables = {table for table, _ in file_table_pairs}
            for table in unique_tables:
                found, version = await registry.get_table_version(table)
                if not found or not version:
                    continue
                try:
                    schema = await registry.get_schema(table, version)
                    await driver.migrate_new_columns(schema, schema.columns())
                except Exception as exc:  # noqa: BLE001
                    _log.debug("[import] Schema reconciliation skipped for %s: %s", table, exc)

        if not dry_run and driver.supports_direct_import() and file_table_pairs:
            await import_files_direct(driver, file_table_pairs)
        else:
            await import_files(
                driver=driver,
                files=files,
                parallel=parallel,
                dry_run=dry_run,
                tracker=tracker,
                checkpoint=checkpoint,
            )

        # Clear checkpoint on success
        await tracker.delete_keys("import:checkpoint", "import:table-export")
        _log.info("[import] Import complete")

    finally:
        await tracker.close()
        if cleanup_dir and import_dir:
            import shutil
            shutil.rmtree(import_dir, ignore_errors=True)
