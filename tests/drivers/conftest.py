"""Session-scoped container fixtures shared by all driver integration tests.

Each fixture starts a real Docker container once per test session and tears it
down afterwards.  Tests that require a container will be skipped automatically
if Docker is unavailable or (for SQL Server) if the ODBC driver is missing.
"""

from __future__ import annotations

import time

import pytest

_PG_USER = "test"
_PG_PASS = "test"
_PG_DB = "testdb"

_MY_USER = "test"
_MY_PASS = "test"
_MY_DB = "testdb"

_SS_PASS = "yourStrong(!)Password"


# ── PostgreSQL ────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def postgres_url():
    try:
        from testcontainers.postgres import PostgresContainer
    except ImportError:
        pytest.skip("testcontainers[postgres] not installed")

    container = PostgresContainer(
        image="postgres:16-alpine",
        username=_PG_USER,
        password=_PG_PASS,
        dbname=_PG_DB,
    )
    container.start()
    host = container.get_container_host_ip()
    port = container.get_exposed_port(5432)
    yield f"postgres://{_PG_USER}:{_PG_PASS}@{host}:{port}/{_PG_DB}"
    container.stop()


# ── MySQL ─────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def mysql_url():
    try:
        from testcontainers.mysql import MySqlContainer
    except ImportError:
        pytest.skip("testcontainers[mysql] not installed")

    container = MySqlContainer(
        image="mysql:8.0",
        username=_MY_USER,
        password=_MY_PASS,
        dbname=_MY_DB,
    )
    container.start()
    host = container.get_container_host_ip()
    port = container.get_exposed_port(3306)
    yield f"mysql://{_MY_USER}:{_MY_PASS}@{host}:{port}/{_MY_DB}"
    container.stop()


# ── SQL Server ────────────────────────────────────────────────────────────────
# Uses mcr.microsoft.com/azure-sql-edge which supports ARM64 natively.
# Requires pyodbc + "ODBC Driver 18 for SQL Server" to be installed locally.

@pytest.fixture(scope="session")
def sqlserver_url():
    try:
        import pyodbc  # noqa: F401
    except ImportError:
        pytest.skip("pyodbc not installed")

    try:
        import pyodbc as _pyodbc
        if "{ODBC Driver 18 for SQL Server}" not in _pyodbc.drivers():
            pytest.skip("ODBC Driver 18 for SQL Server not installed")
    except Exception:
        pytest.skip("Could not enumerate ODBC drivers")

    try:
        from testcontainers.core.container import DockerContainer
    except ImportError:
        pytest.skip("testcontainers.core not installed")

    container = DockerContainer("mcr.microsoft.com/azure-sql-edge:latest")
    container.with_env("ACCEPT_EULA", "Y")
    container.with_env("SA_PASSWORD", _SS_PASS)
    container.with_env("MSSQL_SA_PASSWORD", _SS_PASS)
    container.with_exposed_ports(1433)
    container.start()
    # Allow the engine a moment to finish initialising after the port opens.
    time.sleep(8)
    host = container.get_container_host_ip()
    port = container.get_exposed_port(1433)
    yield (
        f"sqlserver://sa:{_SS_PASS}@{host}:{port}"
        f"?database=master&trust-server-certificate=true"
    )
    container.stop()
