from __future__ import annotations

from typing import Protocol


class StorageTransferPort(Protocol):
    """Port for managing storage transfer jobs."""

    async def create_job(
        self,
        source_bucket: str,
        dest_bucket: str,
        include_prefixes: list[str] | None = None,
    ) -> str: ...

    async def get_job_status(self, job_id: str) -> dict: ...

    async def list_jobs(self) -> list[dict]: ...
