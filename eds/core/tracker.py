"""Tracker abstract base — mirrors internal/tracker/tracker.go."""

from __future__ import annotations

from abc import ABC, abstractmethod


class Tracker(ABC):

    @abstractmethod
    async def get_key(self, key: str) -> str | None: ...

    @abstractmethod
    async def set_key(self, key: str, value: str) -> None: ...

    @abstractmethod
    async def delete_keys(self, *keys: str) -> None: ...

    @abstractmethod
    async def close(self) -> None: ...
