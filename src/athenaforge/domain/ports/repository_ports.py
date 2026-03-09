from __future__ import annotations

from typing import Protocol, TypeVar

T = TypeVar("T")


class ReadRepositoryPort(Protocol[T]):
    """Port for read-only repository operations."""

    async def get_by_id(self, id: str) -> T | None: ...

    async def list_all(self) -> list[T]: ...


class WriteRepositoryPort(Protocol[T]):
    """Port for write repository operations."""

    async def save(self, entity: T) -> None: ...

    async def delete(self, id: str) -> None: ...
