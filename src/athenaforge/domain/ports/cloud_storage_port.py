from __future__ import annotations

from typing import Protocol


class CloudStoragePort(Protocol):
    """Port for interacting with cloud object storage."""

    async def list_objects(self, bucket: str, prefix: str) -> list[str]: ...

    async def read_object(self, bucket: str, key: str) -> bytes: ...

    async def write_object(self, bucket: str, key: str, data: bytes) -> None: ...

    async def get_object_size(self, bucket: str, key: str) -> int: ...
