from __future__ import annotations

import json
import os
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

    def _path_for(self, wave_id: str) -> str:
        return os.path.join(self._dir, f"{wave_id}.json")

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
        path = self._path_for(entity.wave_id)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(self._serialize(entity), fh, indent=2)

    async def delete(self, id: str) -> None:
        path = self._path_for(id)
        if os.path.exists(path):
            os.remove(path)

    # ── ReadRepositoryPort ───────────────────────────────────────

    async def get_by_id(self, id: str) -> Wave | None:
        path = self._path_for(id)
        if not os.path.exists(path):
            return None
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        return self._deserialize(data)

    async def list_all(self) -> list[Wave]:
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
