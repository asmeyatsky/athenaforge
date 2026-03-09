from __future__ import annotations

import json
import os
from typing import Any

from athenaforge.domain.entities.translation_batch import (
    TranslationBatch,
    TranslationFile,
)


class TranslationBatchRepository:
    """JSON file-based repository for TranslationBatch aggregates.

    Implements both ReadRepositoryPort[TranslationBatch] and
    WriteRepositoryPort[TranslationBatch].
    """

    def __init__(self, data_dir: str) -> None:
        self._dir = os.path.join(data_dir, "translation_batches")
        os.makedirs(self._dir, exist_ok=True)

    # ── helpers ──────────────────────────────────────────────────

    def _path_for(self, batch_id: str) -> str:
        return os.path.join(self._dir, f"{batch_id}.json")

    @staticmethod
    def _serialize(entity: TranslationBatch) -> dict[str, Any]:
        files = [
            {
                "file_path": f.file_path,
                "status": f.status,
                "error": f.error,
                "translated_path": f.translated_path,
            }
            for f in entity.files
        ]

        return {
            "batch_id": entity.batch_id,
            "files": files,
            "status": entity.status,
        }

    @staticmethod
    def _deserialize(data: dict[str, Any]) -> TranslationBatch:
        files = tuple(
            TranslationFile(
                file_path=f["file_path"],
                status=f.get("status", "pending"),
                error=f.get("error"),
                translated_path=f.get("translated_path"),
            )
            for f in data.get("files", [])
        )

        return TranslationBatch(
            batch_id=data["batch_id"],
            files=files,
            status=data.get("status", "pending"),
        )

    # ── WriteRepositoryPort ──────────────────────────────────────

    async def save(self, entity: TranslationBatch) -> None:
        path = self._path_for(entity.batch_id)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(self._serialize(entity), fh, indent=2)

    async def delete(self, id: str) -> None:
        path = self._path_for(id)
        if os.path.exists(path):
            os.remove(path)

    # ── ReadRepositoryPort ───────────────────────────────────────

    async def get_by_id(self, id: str) -> TranslationBatch | None:
        path = self._path_for(id)
        if not os.path.exists(path):
            return None
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        return self._deserialize(data)

    async def list_all(self) -> list[TranslationBatch]:
        results: list[TranslationBatch] = []
        if not os.path.isdir(self._dir):
            return results
        for filename in sorted(os.listdir(self._dir)):
            if not filename.endswith(".json"):
                continue
            with open(os.path.join(self._dir, filename), encoding="utf-8") as fh:
                data = json.load(fh)
            results.append(self._deserialize(data))
        return results
