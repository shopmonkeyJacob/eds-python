"""Unit tests for eds.infrastructure.alerting.

Delivery functions (_send_slack, _send_pagerduty, etc.) are patched at the
module level so no real HTTP or SMTP connections are made. The tests focus on
the routing and cooldown logic inside AlertManager.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from eds.infrastructure.alerting import (
    SEVERITY_CRITICAL,
    SEVERITY_WARNING,
    Alert,
    AlertManager,
    ChannelConfig,
)


# ── Severity filtering ────────────────────────────────────────────────────────


async def test_wrong_severity_channel_not_called() -> None:
    """A channel configured only for 'critical' must not fire on a 'warning' alert."""
    ch = ChannelConfig(type="slack", url="http://fake", severity=[SEVERITY_CRITICAL])
    mgr = AlertManager([ch])

    with patch("eds.infrastructure.alerting._send_slack", new_callable=AsyncMock) as mock_send:
        await mgr.fire(Alert("title", "body", severity=SEVERITY_WARNING))
        mock_send.assert_not_called()


async def test_matching_severity_channel_called() -> None:
    ch = ChannelConfig(type="slack", url="http://fake", severity=[SEVERITY_CRITICAL])
    mgr = AlertManager([ch])

    with patch("eds.infrastructure.alerting._send_slack", new_callable=AsyncMock) as mock_send:
        await mgr.fire(Alert("title", "body", severity=SEVERITY_CRITICAL))
        mock_send.assert_called_once()


async def test_multi_severity_channel_receives_both() -> None:
    ch = ChannelConfig(type="slack", url="http://fake", severity=[SEVERITY_WARNING, SEVERITY_CRITICAL])
    mgr = AlertManager([ch], cooldown_seconds=0)

    with patch("eds.infrastructure.alerting._send_slack", new_callable=AsyncMock) as mock_send:
        await mgr.fire(Alert("w", "body", severity=SEVERITY_WARNING))
        await mgr.fire(Alert("c", "body", severity=SEVERITY_CRITICAL))
        assert mock_send.call_count == 2


# ── Cooldown deduplication ────────────────────────────────────────────────────


async def test_cooldown_suppresses_duplicate() -> None:
    """Same alert title on the same channel within the cooldown window is sent only once."""
    ch = ChannelConfig(type="slack", url="http://fake", severity=[SEVERITY_CRITICAL])
    mgr = AlertManager([ch], cooldown_seconds=300)

    with patch("eds.infrastructure.alerting._send_slack", new_callable=AsyncMock) as mock_send:
        await mgr.fire(Alert("same-title", "body", severity=SEVERITY_CRITICAL))
        await mgr.fire(Alert("same-title", "body", severity=SEVERITY_CRITICAL))
        assert mock_send.call_count == 1


async def test_zero_cooldown_sends_every_time() -> None:
    ch = ChannelConfig(type="slack", url="http://fake", severity=[SEVERITY_CRITICAL])
    mgr = AlertManager([ch], cooldown_seconds=0)

    with patch("eds.infrastructure.alerting._send_slack", new_callable=AsyncMock) as mock_send:
        await mgr.fire(Alert("same-title", "body", severity=SEVERITY_CRITICAL))
        await mgr.fire(Alert("same-title", "body", severity=SEVERITY_CRITICAL))
        assert mock_send.call_count == 2


async def test_different_titles_bypass_cooldown_independently() -> None:
    ch = ChannelConfig(type="slack", url="http://fake", severity=[SEVERITY_CRITICAL])
    mgr = AlertManager([ch], cooldown_seconds=300)

    with patch("eds.infrastructure.alerting._send_slack", new_callable=AsyncMock) as mock_send:
        await mgr.fire(Alert("alert-1", "body", severity=SEVERITY_CRITICAL))
        await mgr.fire(Alert("alert-2", "body", severity=SEVERITY_CRITICAL))
        assert mock_send.call_count == 2


async def test_cooldown_is_per_channel() -> None:
    """Two channels have independent cooldown buckets for the same title."""
    ch1 = ChannelConfig(type="slack",   url="http://ch1", severity=[SEVERITY_CRITICAL])
    ch2 = ChannelConfig(type="webhook", url="http://ch2", severity=[SEVERITY_CRITICAL])
    mgr = AlertManager([ch1, ch2], cooldown_seconds=300)

    with (
        patch("eds.infrastructure.alerting._send_slack",   new_callable=AsyncMock) as s,
        patch("eds.infrastructure.alerting._send_webhook", new_callable=AsyncMock) as w,
    ):
        await mgr.fire(Alert("same-title", "body", severity=SEVERITY_CRITICAL))
        assert s.call_count == 1
        assert w.call_count == 1

        # Second fire — both suppressed independently.
        await mgr.fire(Alert("same-title", "body", severity=SEVERITY_CRITICAL))
        assert s.call_count == 1
        assert w.call_count == 1


# ── No channels configured ────────────────────────────────────────────────────


async def test_no_channels_is_noop() -> None:
    mgr = AlertManager([], cooldown_seconds=300)
    await mgr.fire(Alert("title", "body", severity=SEVERITY_CRITICAL))  # must not raise


# ── Channel errors do not propagate ──────────────────────────────────────────


async def test_channel_error_does_not_propagate() -> None:
    """A failing channel must not raise from fire() — errors are logged only."""
    ch = ChannelConfig(type="slack", url="http://fake", severity=[SEVERITY_CRITICAL])
    mgr = AlertManager([ch])

    with patch(
        "eds.infrastructure.alerting._send_slack",
        new_callable=AsyncMock,
        side_effect=RuntimeError("network error"),
    ):
        await mgr.fire(Alert("title", "body", severity=SEVERITY_CRITICAL))  # must not raise


async def test_one_channel_fails_other_still_receives() -> None:
    """When the first channel raises, the second must still be called."""
    ch1 = ChannelConfig(type="slack",   url="http://bad",  severity=[SEVERITY_CRITICAL])
    ch2 = ChannelConfig(type="webhook", url="http://good", severity=[SEVERITY_CRITICAL])
    mgr = AlertManager([ch1, ch2], cooldown_seconds=0)

    with (
        patch("eds.infrastructure.alerting._send_slack",   new_callable=AsyncMock, side_effect=RuntimeError("bad")) as s,
        patch("eds.infrastructure.alerting._send_webhook", new_callable=AsyncMock) as w,
    ):
        await mgr.fire(Alert("title", "body", severity=SEVERITY_CRITICAL))
        s.assert_called_once()
        w.assert_called_once()


# ── Unknown channel type ──────────────────────────────────────────────────────


async def test_unknown_channel_type_does_not_raise() -> None:
    ch = ChannelConfig(type="carrier-pigeon", severity=[SEVERITY_CRITICAL])
    mgr = AlertManager([ch])
    await mgr.fire(Alert("title", "body", severity=SEVERITY_CRITICAL))  # must not raise
