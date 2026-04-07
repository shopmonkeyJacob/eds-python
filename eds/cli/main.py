"""Root CLI group — entry point for the `eds` command."""

import logging
import sys
from pathlib import Path

import click

from eds.cli.server import server
from eds.cli.import_ import import_cmd
from eds.cli.enroll import enroll
from eds.cli.driver import driver


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(level=level, format=fmt, stream=sys.stderr)
    # Quieten noisy third-party loggers
    for lib in ("nats", "azure", "botocore", "urllib3", "asyncio"):
        logging.getLogger(lib).setLevel(logging.WARNING)


@click.group()
@click.option("-d", "--data-dir", default="data", show_default=True,
              help="Directory for state, logs, and credentials.")
@click.option("-v", "--verbose", is_flag=True, default=False,
              help="Enable debug-level console output.")
@click.version_option(package_name="eds", prog_name="eds")
@click.pass_context
def cli(ctx: click.Context, data_dir: str, verbose: bool) -> None:
    """Shopmonkey Enterprise Data Streaming — Python."""
    ctx.ensure_object(dict)
    ctx.obj["data_dir"] = data_dir
    ctx.obj["verbose"] = verbose
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    _configure_logging(verbose)


cli.add_command(server)
cli.add_command(import_cmd, name="import")
cli.add_command(enroll)
cli.add_command(driver)


@cli.command()
@click.pass_context
def version(ctx: click.Context) -> None:
    """Print the current EDS version."""
    from eds.version import CURRENT
    click.echo(CURRENT)


@cli.command()
def publickey() -> None:
    """Print the Shopmonkey PGP public key used to verify upgrades."""
    from eds.public_key import PUBLIC_KEY
    click.echo(PUBLIC_KEY)
