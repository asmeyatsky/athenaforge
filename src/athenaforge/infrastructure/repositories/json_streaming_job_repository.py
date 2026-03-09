from __future__ import annotations

import json
import os
from typing import Any

from athenaforge.domain.entities.streaming_job import StreamingJob


class StreamingJobRepository:
    """JSON file-based repository for StreamingJob aggregates.

    Implements both ReadRepositoryPort[StreamingJob] and
    WriteRepositoryPort[StreamingJob].
    """

    def __init__(self, data_dir: str) -> None:
        self._dir = os.path.join(data_dir, "streaming_jobs")
        os.makedirs(self._dir, exist_ok=True)

    # ── helpers ──────────────────────────────────────────────────

    def _path_for(self, job_id: str) -> str:
        return os.path.join(self._dir, f"{job_id}.json")

    @staticmethod
    def _serialize(entity: StreamingJob) -> dict[str, Any]:
        return {
            "job_id": entity.job_id,
            "source_topic": entity.source_topic,
            "target_topic": entity.target_topic,
            "consumer_group": entity.consumer_group,
            "status": entity.status,
            "lag_threshold": entity.lag_threshold,
            "current_lag": entity.current_lag,
        }

    @staticmethod
    def _deserialize(data: dict[str, Any]) -> StreamingJob:
        return StreamingJob(
            job_id=data["job_id"],
            source_topic=data["source_topic"],
            target_topic=data["target_topic"],
            consumer_group=data["consumer_group"],
            status=data.get("status", "active"),
            lag_threshold=data.get("lag_threshold", 1000),
            current_lag=data.get("current_lag", 0),
        )

    # ── WriteRepositoryPort ──────────────────────────────────────

    async def save(self, entity: StreamingJob) -> None:
        path = self._path_for(entity.job_id)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(self._serialize(entity), fh, indent=2)

    async def delete(self, id: str) -> None:
        path = self._path_for(id)
        if os.path.exists(path):
            os.remove(path)

    # ── ReadRepositoryPort ───────────────────────────────────────

    async def get_by_id(self, id: str) -> StreamingJob | None:
        path = self._path_for(id)
        if not os.path.exists(path):
            return None
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        return self._deserialize(data)

    async def list_all(self) -> list[StreamingJob]:
        results: list[StreamingJob] = []
        if not os.path.isdir(self._dir):
            return results
        for filename in sorted(os.listdir(self._dir)):
            if not filename.endswith(".json"):
                continue
            with open(os.path.join(self._dir, filename), encoding="utf-8") as fh:
                data = json.load(fh)
            results.append(self._deserialize(data))
        return results
