"""eds enroll — register with Shopmonkey HQ via API key or enrollment code."""

from __future__ import annotations

import asyncio
import logging
import uuid

import aiohttp
import click

from eds.infrastructure.config import save_config
from eds.version import CURRENT

_log = logging.getLogger(__name__)

_DEFAULT_API_URL = "https://api.shopmonkey.cloud"


@click.command()
@click.option("--api-key", envvar="EDS_TOKEN", default="",
              help="Shopmonkey API key (skips interactive enrollment code prompt).")
@click.option("--api-url", default=_DEFAULT_API_URL, hidden=True)
@click.option("--code", default="", help="One-time enrollment code from the HQ web interface.")
@click.pass_context
def enroll(ctx: click.Context, api_key: str, api_url: str, code: str) -> None:
    """Save API credentials and register this server with Shopmonkey HQ."""
    data_dir: str = ctx.obj["data_dir"]
    asyncio.run(_enroll(data_dir, api_key, api_url, code))


async def _enroll(data_dir: str, api_key: str, api_url: str, code: str) -> None:
    server_id = str(uuid.uuid4())

    if api_key:
        _log.info("Saving credentials for server %s", server_id)
        save_config(data_dir, api_key, server_id)
        click.echo(f"Enrolled. Server ID: {server_id}")
        return

    if not code:
        code = click.prompt("Enter enrollment code from the Shopmonkey HQ web interface")

    _log.info("Enrolling with code %s", code)
    headers = {
        "User-Agent": f"Shopmonkey EDS Server/{CURRENT}",
        "Content-Type": "application/json",
    }
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.post(
            f"{api_url}/v3/eds/internal/enroll/{code}",
            json={"serverId": server_id},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
            if not data.get("success"):
                raise click.ClickException(f"Enrollment failed: {data.get('message', 'unknown')}")
            token: str = data["data"]["token"]

    save_config(data_dir, token, server_id)
    click.echo(f"Enrolled successfully. Server ID: {server_id}")
    _log.info("Credentials saved to %s/config.toml", data_dir)
