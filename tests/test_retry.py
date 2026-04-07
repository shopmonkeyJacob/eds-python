"""Unit tests for the retry helper."""

import pytest
import asyncio
from eds.core.retry import execute, is_transient


def test_is_transient_connection_reset() -> None:
    assert is_transient(Exception("connection reset by peer"))


def test_is_transient_timeout() -> None:
    assert is_transient(Exception("timeout exceeded"))


def test_is_transient_non_transient() -> None:
    assert not is_transient(ValueError("bad argument"))


@pytest.mark.asyncio
async def test_execute_succeeds_immediately() -> None:
    calls = 0

    async def fn() -> str:
        nonlocal calls
        calls += 1
        return "ok"

    result = await execute(fn, operation_name="test")
    assert result == "ok"
    assert calls == 1


@pytest.mark.asyncio
async def test_execute_retries_on_transient() -> None:
    calls = 0

    async def fn() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise Exception("connection reset by peer")
        return "done"

    result = await execute(fn, operation_name="test", base_delay=0.001)
    assert result == "done"
    assert calls == 3


@pytest.mark.asyncio
async def test_execute_raises_non_transient_immediately() -> None:
    calls = 0

    async def fn() -> str:
        nonlocal calls
        calls += 1
        raise ValueError("fatal")

    with pytest.raises(ValueError, match="fatal"):
        await execute(fn, operation_name="test")

    assert calls == 1


@pytest.mark.asyncio
async def test_execute_raises_after_max_attempts() -> None:
    calls = 0

    async def fn() -> str:
        nonlocal calls
        calls += 1
        raise Exception("timeout exceeded")

    with pytest.raises(Exception, match="timeout exceeded"):
        await execute(fn, operation_name="test", max_attempts=3, base_delay=0.001)

    assert calls == 3
