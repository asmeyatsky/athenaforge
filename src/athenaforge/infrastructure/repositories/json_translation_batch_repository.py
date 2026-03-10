from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
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

    @staticmethod
    def _validate_entity_id(entity_id: str) -> None:
        if not entity_id or '\0' in entity_id or '/' in entity_id or '\\' in entity_id or '..' in entity_id:
            raise ValueError(f"Invalid entity ID: '{entity_id}'")

    def _path_for(self, entity_id: str) -> Path:
        self._validate_entity_id(entity_id)
        path = Path(self._dir) / f"{entity_id}.json"
        resolved = path.resolve()
        if not resolved.is_relative_to(Path(self._dir).resolve()):
            raise ValueError(f"Invalid entity ID: path traversal detected in '{entity_id}'")
        return resolved

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
        def _save():
            path = self._path_for(entity.batch_id)
            data = self._serialize(entity)
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(data, fh, indent=2)
        await asyncio.to_thread(_save)

    async def delete(self, id: str) -> None:
        def _delete():
            path = self._path_for(id)
            if path.exists():
                path.unlink()
        await asyncio.to_thread(_delete)

    # ── ReadRepositoryPort ───────────────────────────────────────

    async def get_by_id(self, id: str) -> TranslationBatch | None:
        def _get():
            path = self._path_for(id)
            if not path.exists():
                return None
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
            return self._deserialize(data)
        return await asyncio.to_thread(_get)

    async def list_all(self) -> list[TranslationBatch]:
        def _list():
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
        return await asyncio.to_thread(_list)
