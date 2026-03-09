from __future__ import annotations

from uuid import uuid4


class StsTransferAdapter:
    """Implements StorageTransferPort using GCP Storage Transfer Service.

    All methods are async stubs; a real implementation would use the
    ``google-cloud-storage-transfer`` client library.
    """

    def __init__(self, project_id: str) -> None:
        self._project_id = project_id

    async def create_job(
        self,
        source_bucket: str,
        dest_bucket: str,
        include_prefixes: list[str] | None = None,
    ) -> str:
        """Return a generated job ID (stub)."""
        job_id = f"transferJobs/{uuid4().hex[:12]}"
        return job_id

    async def get_job_status(self, job_id: str) -> dict:
        return {
            "job_id": job_id,
            "project_id": self._project_id,
            "status": "PENDING",
        }

    async def list_jobs(self) -> list[dict]:
        return []
