# EDS — Enterprise Data Streaming (Python)

A Python port of Shopmonkey's Enterprise Data Streaming server. Connects to Shopmonkey HQ via NATS JetStream and streams change data capture (CDC) events to your own data infrastructure through a configurable driver.

## Supported Drivers

| Scheme       | Destination              | Import | Time-series | Add col | Change type | Drop col |
|--------------|--------------------------|:------:|:-----------:|:-------:|:-----------:|:--------:|
| `postgres`   | PostgreSQL / CockroachDB | ✓      | ✓           | ✓       | ✓           | ✓        |
| `mysql`      | MySQL / MariaDB          | ✓      | ✓           | ✓       | ✓           | ✓        |
| `sqlserver`  | SQL Server               | ✓      | ✓           | ✓       | ✓           | ✓        |
| `snowflake`  | Snowflake                | ✓      | —           | ✓       | —           | —        |
| `s3`         | Amazon S3                | ✓ ²    | —           | —       | —           | —        |
| `azureblob`  | Azure Blob Storage       | ✓ ²    | —           | —       | —           | —        |
| `file`       | Local NDJSON files       | ✓ ²    | —           | —       | —           | —        |
| `kafka`      | Apache Kafka             | ✓ ³    | —           | —       | —           | —        |
| `eventhub`   | Azure Event Hubs         | ✓ ³    | —           | —       | —           | —        |

- **Time-series** — supports `--driver-mode timeseries` (append-only event log with auto-maintained views)
- **Add col / Change type / Drop col** — automatic DDL migrations when the Shopmonkey schema evolves

> ² S3, Azure Blob Storage, and File drivers transfer raw `.ndjson.gz` export files directly to the destination, preserving the filename and per-table directory structure. No row-level parsing is performed — the export format is already the natural storage format for these drivers.
>
> ³ Kafka and EventHub drivers parse each `.ndjson.gz` export file and publish every row as an individual message to the configured topic/hub, using the same batching and partition-key logic as the live CDC stream.

## Requirements

- A Shopmonkey account with EDS access
- **Pre-built binaries:** no additional requirements
- **Install from source:** Python 3.11+

## Installation

### Pre-built binaries (recommended)

Download the latest release from the [GitHub Releases](../../releases) page:

| Platform            | Asset                    |
|---------------------|--------------------------|
| macOS (Apple Silicon) | `eds-osx-arm64.tar.gz` |
| Linux x64           | `eds-linux-x64.tar.gz`   |
| Windows x64         | `eds-win-x64.zip`        |

Extract and run:

```sh
# macOS / Linux
tar xzf eds-osx-arm64.tar.gz
./eds server

# Windows (PowerShell)
Expand-Archive eds-win-x64.zip .
.\eds.exe server
```

### Install from source

```sh
pip install .
```

It is recommended to install inside a virtual environment:

```sh
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install .
```

For development (includes linting, type checking, and test dependencies):

```sh
pip install ".[dev]"
```

## Quick Start

1. Install using one of the methods above.
2. Run `eds server` — on first launch you will be prompted for a one-time enrollment code from the [Shopmonkey HQ web interface](https://app.shopmonkey.io).
3. Configure your destination driver in the web interface. EDS will begin streaming events as soon as a driver URL is saved.

## Commands

| Command                            | Description                                                  |
|------------------------------------|--------------------------------------------------------------|
| `eds server`                       | Start the streaming server                                   |
| `eds import`                       | Run a one-time bulk data import                              |
| `eds enroll --api-key <token>`     | Save API credentials directly without an enrollment code     |
| `eds driver list`                  | List all available drivers                                   |
| `eds driver help <scheme>`         | Show driver-specific connection string help                  |
| `eds version`                      | Print the current EDS version                                |
| `eds publickey`                    | Print the Shopmonkey PGP public key used to verify upgrades  |

### Global options

| Flag              | Description                                                          |
|-------------------|----------------------------------------------------------------------|
| `-d, --data-dir`  | Directory for state, logs, and credentials (default: `data`)         |
| `-v, --verbose`   | Enable debug-level console output                                    |

### `eds server` options

| Flag              | Description                                                                    |
|-------------------|--------------------------------------------------------------------------------|
| `--api-key`       | Shopmonkey API key (or set `EDS_TOKEN` env var)                                |
| `--url`           | Driver connection URL (or set `EDS_URL` env var)                               |
| `--driver-mode`   | `upsert` (default) or `timeseries` — see [Time-Series Mode](#time-series-mode) |
| `--events-schema` | Schema name for time-series events tables (default: `eds_events`)              |

### `eds import` options

| Flag              | Description                                                                    |
|-------------------|--------------------------------------------------------------------------------|
| `--url`           | Destination driver URL (falls back to `url` in config.toml)                   |
| `--api-key`       | Shopmonkey API key (falls back to `token` in config.toml)                      |
| `--only`          | Comma-separated list of table names to import                                  |
| `--company-ids`   | Filter export to specific company IDs                                           |
| `--location-ids`  | Filter export to specific location IDs                                          |
| `--job-id`        | Reuse an existing export job ID                                                 |
| `--dir`           | Path to already-downloaded export files (skips API export)                     |
| `--parallel`      | Max parallel table workers (default: 4)                                         |
| `--dry-run`       | Parse and validate without writing any rows                                     |
| `--no-confirm`    | Skip the interactive delete confirmation prompt                                 |
| `--no-delete`     | Insert rows only; do not drop and recreate tables                               |
| `--schema-only`   | Create tables without importing any rows                                        |
| `--no-cleanup`    | Keep the temporary download directory after import                              |
| `--resume`        | Resume the last interrupted import — re-polls the export if still in progress, skips already-downloaded files, and continues from the first unfinished row file (implies `--no-delete --no-cleanup`) |
| `--driver-mode`   | `upsert` (default) or `timeseries` — see [Time-Series Mode](#time-series-mode) |
| `--events-schema` | Schema name for time-series events tables (default: `eds_events`)              |

#### Import resumability

EDS saves a checkpoint to `data/state.db` as soon as an export job is created. This means `--resume` can recover from an interruption at **any stage** of the import pipeline:

| Interrupted during | `--resume` behaviour |
|--------------------|----------------------|
| Export polling (HQ still generating files) | Re-polls the same export job until it completes, then downloads |
| File download | Skips files already on disk, downloads the remainder |
| Row import | Skips fully-processed files, re-applies the first unfinished file from the start |

```sh
eds import --resume
```

Because rows are written via upsert, re-applying a partially-processed file is safe — duplicate rows are simply overwritten. The checkpoint is automatically cleared after a successful full import.

You can also specify an export job ID explicitly to attach to a known job without needing a saved checkpoint:

```sh
eds import --job-id <export-job-id>
```

#### Export timeout recovery

Large exports can time out on the Shopmonkey side before all tables have finished generating. EDS handles this automatically:

1. Any tables that already have download URLs are fetched immediately.
2. A new targeted export job is created for the remaining tables.
3. Steps 1–2 repeat up to **5 times** with exponential back-off between attempts:

| Attempt | Delay before retry |
|---------|--------------------|
| 1       | 30 s               |
| 2       | 60 s               |
| 3       | 120 s              |
| 4       | 240 s              |
| 5       | 480 s              |

If all 5 retries are exhausted without completing every table, the import exits with a fatal error and the partial results are preserved on disk so `--resume` can continue from where it left off.

## Time-Series Mode

By default, SQL drivers mirror the source tables using **upsert** semantics — each row reflects the latest known state of the corresponding record in Shopmonkey.

Passing `--driver-mode timeseries` switches all SQL drivers to an **append-only event log** model. Every CDC event is inserted as a new row; no rows are ever updated or deleted. This enables full audit history and point-in-time queries at the cost of needing to join against the latest event per entity when you want current state.

### Events table schema

For each Shopmonkey table `{table}`, a fixed-schema table is created:

| Dialect         | Table location                     |
|-----------------|------------------------------------|
| PostgreSQL      | `eds_events.{table}_events`        |
| MySQL / MariaDB | `{table}__events` (same database)  |
| SQL Server      | `eds_events.{table}_events`        |
| Snowflake       | `eds_events.{table}_events`        |

All events tables share the same columns regardless of the source table structure:

| Column        | Type   | Description                                               |
|---------------|--------|-----------------------------------------------------------|
| `_seq`        | BIGINT | Auto-increment primary key (insertion order)              |
| `_event_id`   | TEXT   | Unique event ID from Shopmonkey                           |
| `_operation`  | TEXT   | `CREATE`, `UPDATE`, or `DELETE`                           |
| `_entity_id`  | TEXT   | Primary key of the affected record                        |
| `_timestamp`  | BIGINT | Event timestamp in milliseconds since epoch               |
| `_mvcc_ts`    | TEXT   | MVCC timestamp from the source database                   |
| `_company_id` | TEXT   | Shopmonkey company ID                                     |
| `_location_id`| TEXT   | Shopmonkey location ID                                    |
| `_model_ver`  | TEXT   | Shopmonkey schema model version                           |
| `_diff`       | TEXT   | JSON array of changed field names (UPDATE only)           |
| `_before`     | JSON   | Full record state before the change (JSONB on PostgreSQL) |
| `_after`      | JSON   | Full record state after the change (JSONB on PostgreSQL)  |

### Auto-maintained views

Three views are automatically created and refreshed whenever the schema changes:

**`current_{table}`** — latest state of each entity (equivalent to the upsert mirror):

```sql
-- Example: latest state of all work orders
SELECT * FROM eds_events.current_work_orders;
```

**`{table}_history`** — full audit trail with each schema column extracted from the JSON payloads:

```sql
-- Example: full change history for a specific work order
SELECT _seq, _operation, _timestamp, id, status, total
FROM eds_events.work_orders_history
WHERE id = 'wo-abc123'
ORDER BY _timestamp;
```

**`{table}_unified`** — complete current dataset combining CDC events with the mirror table baseline. Records that have received CDC events use the event-derived state; records that exist only in the mirror table (e.g. rows imported before the live server began streaming) are surfaced directly from the mirror, so the full dataset is always visible:

```sql
-- Complete current state, including both CDC-updated and import-only rows
SELECT * FROM eds_events.work_orders_unified;
```

> The `{table}_unified` view is only created once the standard mirror table exists (i.e. after a bulk import has been run). If no import has been performed, only `current_{table}` and `{table}_history` are created. The view is automatically created on the next `eds server` start once the mirror table is present.

### Point-in-time queries

To reconstruct the state of all records at a specific moment, use a window function directly against the events table:

```sql
-- State of all work orders as of a specific Unix millisecond timestamp
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY _entity_id ORDER BY _timestamp DESC, _seq DESC) AS rn
  FROM eds_events.work_orders_events
  WHERE _timestamp <= 1743897600000   -- 2025-02-06T00:00:00Z
) t
WHERE rn = 1 AND _operation <> 'DELETE';
```

### Joining across tables in time-series mode

When joining time-series tables, use the auto-maintained views to get current state on both sides:

```sql
-- Current work orders joined to current customer
SELECT wo.id, wo.status, c.name AS customer_name
FROM eds_events.current_work_orders wo
JOIN eds_events.current_customers c ON c.id = wo.customer_id;
```

### Usage

```sh
# Start the server in time-series mode
eds server --driver-mode timeseries

# Run a bulk import then start the server in time-series mode
eds import --driver-mode timeseries --url postgres://user:password@localhost/mydb
```

The selected mode is automatically persisted to `config.toml` (`driver_mode = "timeseries"`). On subsequent restarts you can omit the flag and the stored value will be used. If you pass `--driver-mode` with a value that differs from what's in `config.toml`, EDS will prompt you to confirm before changing it. Pass `--no-confirm` to accept the change non-interactively (useful in scripts).

> **Note on bulk import:** Regardless of `--driver-mode`, the `eds import` command always writes the snapshot data into the **standard mirror tables** (e.g. `order`, `customer`). The events tables (`eds_events.order_events` etc.) are populated only by the live CDC stream once `eds server` is running. This means you can safely run `eds import --driver-mode timeseries` to set up the mode and load the initial snapshot — the server will then append new change events to the events tables on top of that baseline.

> **Note:** Upsert and time-series data can coexist in the same database. The events tables live in the `eds_events` schema (or use a `__events` suffix in MySQL), keeping them separate from the standard mirror tables.

## Driver Connection Strings

### PostgreSQL (`postgres`)

```
postgres://user:password@host:5432/dbname
postgres://user:password@host:5432/dbname?ssl=true
```

Also accepted as `postgresql://`.

### MySQL / MariaDB (`mysql`)

```
mysql://user:password@host:3306/dbname
```

### SQL Server (`sqlserver`)

```
sqlserver://user:password@host:1433?database=dbname
sqlserver://user:password@host:1433?database=dbname&trust-server-certificate=false
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `trust-server-certificate` | `true` | Set to `false` to enforce TLS certificate validation (recommended for production) |

### Snowflake (`snowflake`)

```
snowflake://user:password@account/database/schema?warehouse=WH&role=ROLE
```

### Amazon S3 (`s3`)

```
s3://bucket-name/optional-prefix?region=us-east-1
s3://bucket-name/optional-prefix?region=us-east-1&access-key=KEY&secret-key=SECRET
```

Credentials fall back to the AWS credential chain (environment variables, `~/.aws/credentials`, instance metadata) when `access-key` and `secret-key` are not provided.

### Azure Blob Storage (`azureblob`)

| Auth method          | URL format |
|----------------------|------------|
| Account key          | `azureblob://accountname/containername?key=<base64-key>` |
| Account key + prefix | `azureblob://accountname/containername/myprefix?key=<base64-key>` |
| Connection string    | `azureblob://accountname/containername?connection-string=<uri-encoded-string>` |

### Apache Kafka (`kafka`)

```
kafka://broker1:9092,broker2:9092/topic-name
kafka://broker:9092/topic-name?security.protocol=SASL_SSL&sasl.mechanism=PLAIN&sasl.username=user&sasl.password=pass
```

### Azure Event Hubs (`eventhub`)

```
eventhub://namespace.servicebus.windows.net/hub-name?connection-string=<uri-encoded-string>
```

### Local file (`file`)

```
file:///path/to/output/directory
```

Each table's events are written as NDJSON to `{directory}/{table}/{timestamp}.ndjson`.

## Configuration

At runtime EDS creates a `data/` directory (or the path set by `--data-dir`) containing:

| File / Directory   | Description                                          |
|--------------------|------------------------------------------------------|
| `config.toml`      | Server settings — API token, driver URL, server ID   |
| `state.db`         | SQLite database for change-tracking and import state |
| `<session-id>/`    | NATS credentials for the current session             |

> **Keep `data/` out of source control.** It contains your API token and NATS credentials.

`config.toml` is written with `600` permissions (owner read/write only) on macOS and Linux to protect the API token it contains.

### Example `config.toml`

```toml
token        = "your-shopmonkey-jwt"
server_id    = "your-server-id"
url          = "postgres://user:password@localhost:5432/mydb"
driver_mode  = "upsert"
events_schema = "eds_events"   # optional; must match [A-Za-z0-9_]{1,128}
```

Environment variables prefixed with `EDS_` override any value in `config.toml`:

| Variable        | Overrides      |
|-----------------|----------------|
| `EDS_TOKEN`     | `token`        |
| `EDS_URL`       | `url`          |
| `EDS_SERVER_ID` | `server_id`    |
| `EDS_API_URL`   | `api_url`      |

## Metrics & Status

EDS exposes an HTTP server on port **8080** (configurable via the `[metrics]` section of `config.toml`). By default it binds to `localhost` only. Two endpoints are available:

| Endpoint   | Format     | Description                                              |
|------------|------------|----------------------------------------------------------|
| `/metrics` | Prometheus | Counters and gauges for scraping by Prometheus / Grafana |
| `/status`  | JSON       | Human-readable runtime snapshot                          |

### Configuring the metrics port

```toml
[metrics]
port = 9090   # default: 8080
host = "0.0.0.0"  # expose on all interfaces (needed for Docker/k8s); default is "localhost"
```

## Session Renewal

EDS automatically restarts every 24 hours to obtain a fresh session and NATS credentials from Shopmonkey HQ. The restart is clean — all in-flight events are flushed before shutdown. If you are running EDS under a process supervisor (systemd, Docker, etc.), configure it to restart on any exit code.

## Exit Codes

| Code | Meaning                                                              | Recommended supervisor action |
|------|----------------------------------------------------------------------|-------------------------------|
| `0`  | Clean shutdown                                                       | Do not restart                |
| `1`  | Fatal error                                                          | Restart with backoff          |
| `4`  | Intentional restart (session renewal, HQ-initiated, upgrade)         | Restart immediately           |
| `5`  | NATS connectivity lost                                               | Restart with backoff          |

### systemd example

```ini
[Service]
ExecStart=/usr/local/bin/eds server
Restart=always
RestartSec=5
RestartForceExitStatus=4 5
```

## Building Binaries

Each GitHub release automatically builds self-contained, single-file binaries for all three platforms using [PyInstaller](https://pyinstaller.org). No Python installation is required to run them.

The workflow lives at `.github/workflows/build.yml`. Binaries are produced on a push to a `v*` tag:

```sh
git tag v1.2.3
git push origin v1.2.3
```

This triggers the `publish` matrix job, which builds on the native runner for each platform:

| Platform            | Runner            |
|---------------------|-------------------|
| macOS (Apple Silicon) | `macos-14`      |
| Linux x64           | `ubuntu-latest`   |
| Windows x64         | `windows-latest`  |

The `release` job then collects all three archives and creates a GitHub Release with auto-generated release notes (commits since the previous tag).

To build a binary locally:

```sh
pip install . pyinstaller
pyinstaller --onefile --name eds \
  --hidden-import eds.drivers.postgres \
  --hidden-import eds.drivers.mysql \
  --hidden-import eds.drivers.sqlserver \
  --hidden-import eds.drivers.snowflake_ \
  --hidden-import eds.drivers.s3 \
  --hidden-import eds.drivers.azure_blob \
  --hidden-import eds.drivers.kafka_ \
  --hidden-import eds.drivers.eventhub \
  --hidden-import eds.drivers.file_ \
  eds/__main__.py
# Output: dist/eds  (or dist/eds.exe on Windows)
```

## Running Tests

```sh
pytest
```

The test suite requires no external services. Tests cover CDC event parsing, retry logic, and the SQLite tracker.

## Architecture

```
Shopmonkey HQ
     │  NATS JetStream (CDC events)
     ▼
Consumer  ──▶  Driver  ──▶  Destination (SQL, S3, Kafka, …)
     │
     │  NATS notifications (configure, import, pause, upgrade, …)
     ▼
NotificationService
```

- **CDC events** arrive via NATS JetStream, are buffered in an asyncio queue, and flushed to the driver in batches with exponential-backoff retry on transient failures.
- **Notifications** from HQ allow the web interface to configure the driver, trigger a backfill import, pause/unpause streaming, and initiate in-place binary upgrades.
- **Schema registry** tracks table model versions and triggers DDL migrations when the Shopmonkey data model changes.
- **Pause handling** NAKs messages with a 30-second server-side delay so paused sessions do not create a tight redelivery loop.

## Project Structure

```
eds/
  cli/              CLI commands (server, import, enroll, driver, …)
  core/             Interfaces, models, retry, and tracker abstraction
  drivers/          Driver implementations
    base.py         SQL driver base (upsert + time-series logic)
    postgres.py
    mysql.py
    sqlserver.py
    snowflake_.py
    s3.py
    azure_blob.py
    kafka_.py
    eventhub.py
    file_.py
  importer/         Bulk import pipeline (NDJSON/gz)
  infrastructure/   NATS consumer, schema registry, config, metrics, upgrade
tests/
pyproject.toml
```

## License

Copyright (c) 2022-2026 Shopmonkey, Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
