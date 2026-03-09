from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any

from athenaforge.domain.entities.transfer_job import TransferJob


class TransferJobRepository:
    """JSON file-based repository for TransferJob aggregates.

    Implements both ReadRepositoryPort[TransferJob] and
    WriteRepositoryPort[TransferJob].
    """

    def __init__(self, data_dir: str) -> None:
        self._dir = os.path.join(data_dir, "transfer_jobs")
        os.makedirs(self._dir, exist_ok=True)

    # ── helpers ──────────────────────────────────────────────────

    def _path_for(self, job_id: str) -> str:
        return os.path.join(self._dir, f"{job_id}.json")

    @staticmethod
    def _serialize(entity: TransferJob) -> dict[str, Any]:
        return {
            "job_id": entity.job_id,
            "source_bucket": entity.source_bucket,
            "destination_bucket": entity.destination_bucket,
            "total_bytes": entity.total_bytes,
            "bytes_transferred": entity.bytes_transferred,
            "status": entity.status,
            "created_at": entity.created_at.isoformat(),
        }

    @staticmethod
    def _deserialize(data: dict[str, Any]) -> TransferJob:
        return TransferJob(
            job_id=data["job_id"],
            source_bucket=data["source_bucket"],
            destination_bucket=data["destination_bucket"],
            total_bytes=data["total_bytes"],
            bytes_transferred=data.get("bytes_transferred", 0),
            status=data.get("status", "pending"),
            created_at=datetime.fromisoformat(data["created_at"]),
        )

    # ── WriteRepositoryPort ──────────────────────────────────────

    async def save(self, entity: TransferJob) -> None:
        path = self._path_for(entity.job_id)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(self._serialize(entity), fh, indent=2)

    async def delete(self, id: str) -> None:
        path = self._path_for(id)
        if os.path.exists(path):
            os.remove(path)

    # ── ReadRepositoryPort ───────────────────────────────────────

    async def get_by_id(self, id: str) -> TransferJob | None:
        path = self._path_for(id)
        if not os.path.exists(path):
            return None
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        return self._deserialize(data)

    async def list_all(self) -> list[TransferJob]:
        results: list[TransferJob] = []
        if not os.path.isdir(self._dir):
            return results
        for filename in sorted(os.listdir(self._dir)):
            if not filename.endswith(".json"):
                continue
            with open(os.path.join(self._dir, filename), encoding="utf-8") as fh:
                data = json.load(fh)
            results.append(self._deserialize(data))
        return results
