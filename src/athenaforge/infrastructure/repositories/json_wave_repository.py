from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

from athenaforge.domain.entities.wave import Wave
from athenaforge.domain.value_objects.wave import ParallelRunMode, WaveStatus


class WaveRepository:
    """JSON file-based repository for Wave aggregates.

    Implements both ReadRepositoryPort[Wave] and WriteRepositoryPort[Wave].
    """

    def __init__(self, data_dir: str) -> None:
        self._dir = os.path.join(data_dir, "waves")
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
    def _serialize(entity: Wave) -> dict[str, Any]:
        return {
            "wave_id": entity.wave_id,
            "name": entity.name,
            "lob": entity.lob,
            "tables": list(entity.tables),
            "status": entity.status.value,
            "mode": entity.mode.value,
            "dvt_results": dict(entity.dvt_results),
        }

    @staticmethod
    def _deserialize(data: dict[str, Any]) -> Wave:
        return Wave(
            wave_id=data["wave_id"],
            name=data["name"],
            lob=data["lob"],
            tables=tuple(data.get("tables", [])),
            status=WaveStatus(data.get("status", "PLANNED")),
            mode=ParallelRunMode(data.get("mode", "OLD_ONLY")),
            dvt_results=data.get("dvt_results", {}),
        )

    # ── WriteRepositoryPort ──────────────────────────────────────

    async def save(self, entity: Wave) -> None:
        def _save():
            path = self._path_for(entity.wave_id)
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

    async def get_by_id(self, id: str) -> Wave | None:
        def _get():
            path = self._path_for(id)
            if not path.exists():
                return None
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
            return self._deserialize(data)
        return await asyncio.to_thread(_get)

    async def list_all(self) -> list[Wave]:
        def _list():
            results: list[Wave] = []
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
