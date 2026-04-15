"""Interactive configuration wizard — runs once after fresh enrollment.

Prompts the user to configure server tuning options and writes the chosen
values to config.toml.  Mirrors ConfigWizard.cs in the .NET implementation.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import click

from eds.core.driver import DriverMode
from eds.infrastructure.config import set_config_value


@dataclass
class WizardResult:
    mode: DriverMode
    events_schema: str
    min_pending_latency_secs: int
    max_pending_latency_secs: int
    max_ack_pending: int


_TIMESERIES_LABEL = (
    "timeseries — append every event to an audit log with auto-maintained views (recommended)"
)
_UPSERT_LABEL = (
    "upsert     — mirror source tables, keeping only the latest state of each row"
)

_MODE_CHOICES = {
    "1": (_TIMESERIES_LABEL, DriverMode.TIMESERIES),
    "2": (_UPSERT_LABEL, DriverMode.UPSERT),
}


async def run_wizard(data_dir: str) -> WizardResult:
    """Run the interactive configuration wizard and persist values to config.toml.

    Parameters
    ----------
    data_dir:
        Path to the EDS data directory (contains config.toml).

    Returns
    -------
    WizardResult with the five configured values.
    """
    click.echo()
    click.secho("Server Configuration", bold=True, fg="yellow")
    click.echo(
        "Configure how EDS streams events. Press Enter to accept the default shown in brackets."
    )
    click.echo()

    # ── 1. Driver mode ────────────────────────────────────────────────────────
    click.secho("Driver mode", bold=True, nl=False)
    click.echo(" — how should events be written to your database?")
    click.echo(f"  1) {_TIMESERIES_LABEL}")
    click.echo(f"  2) {_UPSERT_LABEL}")

    mode_key = click.prompt(
        "Choose",
        type=click.Choice(["1", "2"], case_sensitive=False),
        default="1",
        show_choices=False,
    )
    _, mode = _MODE_CHOICES[mode_key]
    mode_str = mode.value
    await set_config_value(data_dir, "driver_mode", mode_str)

    # ── 2. Events schema (timeseries only) ────────────────────────────────────
    events_schema = "eds_events"
    if mode == DriverMode.TIMESERIES:
        events_schema = click.prompt(
            click.style("Events schema", bold=True)
            + " — database schema to hold events tables",
            default="eds_events",
        )
        # Validate schema name
        if not re.match(r"^[A-Za-z0-9_]{1,128}$", events_schema):
            raise click.ClickException(
                f"Invalid events schema name {events_schema!r}. "
                "Must contain only ASCII letters, digits, and underscores (1-128 chars)."
            )
        await set_config_value(data_dir, "events_schema", events_schema)

    # ── 3-5. Flush tuning ─────────────────────────────────────────────────────
    click.echo()
    click.echo(
        click.style("Advanced flush tuning", fg="bright_black")
        + click.style(" — controls how events are batched before writing.", fg="bright_black")
    )

    min_secs = _prompt_positive_int(
        click.style("Min flush interval", bold=True)
        + " (seconds) — minimum time to accumulate events before writing",
        default=2,
    )
    await set_config_value(data_dir, "min_pending_latency", str(min_secs))

    max_secs = _prompt_int_greater_than(
        click.style("Max flush interval", bold=True)
        + " (seconds) — flush is forced after this interval regardless of batch size",
        default=30,
        floor=min_secs,
        floor_label=f"min interval ({min_secs}s)",
    )
    await set_config_value(data_dir, "max_pending_latency", str(max_secs))

    max_ack = _prompt_positive_int(
        click.style("Max batch size", bold=True)
        + " — maximum number of events to accumulate before an immediate flush",
        default=16384,
    )
    await set_config_value(data_dir, "max_ack_pending", str(max_ack))

    click.echo()
    click.secho("Configuration saved.", bold=True, fg="green")
    click.echo()

    return WizardResult(
        mode=mode,
        events_schema=events_schema,
        min_pending_latency_secs=min_secs,
        max_pending_latency_secs=max_secs,
        max_ack_pending=max_ack,
    )


# ── Prompt helpers ────────────────────────────────────────────────────────────


def _prompt_positive_int(text: str, default: int) -> int:
    """Prompt for a positive integer, re-asking on invalid input."""
    while True:
        raw = click.prompt(text, default=default)
        try:
            val = int(raw)
        except (TypeError, ValueError):
            click.echo("Please enter a positive integer.")
            continue
        if val <= 0:
            click.echo("Must be greater than zero.")
            continue
        return val


def _prompt_int_greater_than(
    text: str,
    default: int,
    floor: int,
    floor_label: str,
) -> int:
    """Prompt for an integer strictly greater than *floor*."""
    while True:
        raw = click.prompt(text, default=default)
        try:
            val = int(raw)
        except (TypeError, ValueError):
            click.echo(f"Please enter a positive integer greater than the {floor_label}.")
            continue
        if val <= floor:
            click.echo(f"Must be greater than {floor_label}.")
            continue
        return val
