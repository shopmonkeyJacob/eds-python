"""Driver registry — import this module to auto-register all built-in drivers.

Each driver is registered lazily: if its optional system dependency (ODBC,
Snowflake connector, etc.) is missing the driver is skipped with a warning
rather than crashing the entire process at startup.
"""

import logging

from eds.core.driver import register_driver

_log = logging.getLogger(__name__)


def _try_register(scheme: str, factory: str) -> None:
    """Import *factory* (dotted path) and register the driver for *scheme*."""
    module_path, cls_name = factory.rsplit(".", 1)
    try:
        import importlib
        mod = importlib.import_module(module_path)
        cls = getattr(mod, cls_name)
        register_driver(scheme, cls())
    except ImportError as exc:
        _log.debug("Driver '%s' not available (missing dependency): %s", scheme, exc)


_try_register("postgres",   "eds.drivers.postgres.PostgresDriver")
_try_register("postgresql", "eds.drivers.postgres.PostgresDriver")
_try_register("mysql",      "eds.drivers.mysql.MySQLDriver")
_try_register("sqlserver",  "eds.drivers.sqlserver.SqlServerDriver")
_try_register("snowflake",  "eds.drivers.snowflake_.SnowflakeDriver")
_try_register("s3",         "eds.drivers.s3.S3Driver")
_try_register("azureblob",  "eds.drivers.azure_blob.AzureBlobDriver")
_try_register("kafka",      "eds.drivers.kafka_.KafkaDriver")
_try_register("eventhub",   "eds.drivers.eventhub.EventHubDriver")
_try_register("file",       "eds.drivers.file_.FileDriver")

__all__ = [
    "PostgresDriver",
    "MySQLDriver",
    "SqlServerDriver",
    "SnowflakeDriver",
    "S3Driver",
    "AzureBlobDriver",
    "KafkaDriver",
    "EventHubDriver",
    "FileDriver",
]
