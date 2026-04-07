"""eds driver — list and inspect registered drivers."""

import click

import eds.drivers  # noqa: F401 — triggers auto-registration
from eds.core.driver import get_driver_metadata, _registry


@click.group()
def driver() -> None:
    """Inspect available EDS drivers."""


@driver.command(name="list")
def list_drivers() -> None:
    """List all available drivers."""
    meta = get_driver_metadata()
    if not meta:
        click.echo("No drivers registered.")
        return
    click.echo(f"{'Scheme':<15} {'Name':<20} Description")
    click.echo("-" * 72)
    for m in sorted(meta, key=lambda x: x["scheme"]):
        click.echo(f"{m['scheme']:<15} {m['name']:<20} {m['description']}")


@driver.command(name="help")
@click.argument("scheme")
def driver_help(scheme: str) -> None:
    """Show connection string help for a specific driver scheme."""
    d = _registry.get(scheme)
    if d is None:
        raise click.ClickException(f"No driver registered for scheme '{scheme}'")

    click.echo(f"\n{d.name()}")
    click.echo("=" * len(d.name()))
    if d.description():
        click.echo(d.description())
    if d.example_url():
        click.echo(f"\nExample URL:\n  {d.example_url()}")
    if d.help_text():
        click.echo(f"\n{d.help_text()}")

    fields = d.configuration()
    if fields:
        click.echo("\nConfiguration fields:")
        for f in fields:
            req = "required" if f.required else "optional"
            default = f"  [default: {f.default}]" if f.default else ""
            click.echo(f"  {f.name:<25} ({req}){default}")
            click.echo(f"    {f.description}")
