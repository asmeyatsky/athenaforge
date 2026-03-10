from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from athenaforge.domain.entities.table_inventory import TableEntry, TableInventory
from athenaforge.domain.value_objects.tier import Tier, TierClassification


class TableInventoryRepository:
    """JSON file-based repository for TableInventory aggregates.

    Implements both ReadRepositoryPort[TableInventory] and
    WriteRepositoryPort[TableInventory].
    """

    def __init__(self, data_dir: str) -> None:
        self._dir = os.path.join(data_dir, "table_inventories")
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
    def _serialize(entity: TableInventory) -> dict[str, Any]:
        tables = [
            {
                "table_name": t.table_name,
                "database": t.database,
                "size_bytes": t.size_bytes,
                "row_count": t.row_count,
                "last_queried": t.last_queried.isoformat() if t.last_queried else None,
                "partitioned": t.partitioned,
                "format": t.format,
                "has_maps": t.has_maps,
            }
            for t in entity.tables
        ]

        classifications: dict[str, dict[str, Any]] = {}
        for name, tc in entity.classifications.items():
            classifications[name] = {
                "table_name": tc.table_name,
                "tier": tc.tier.value,
                "reason": tc.reason,
                "size_bytes": tc.size_bytes,
                "last_queried_days_ago": tc.last_queried_days_ago,
            }

        return {
            "inventory_id": entity.inventory_id,
            "tables": tables,
            "classifications": classifications,
        }

    @staticmethod
    def _deserialize(data: dict[str, Any]) -> TableInventory:
        tables = tuple(
            TableEntry(
                table_name=t["table_name"],
                database=t["database"],
                size_bytes=t["size_bytes"],
                row_count=t["row_count"],
                last_queried=(
                    datetime.fromisoformat(t["last_queried"])
                    if t.get("last_queried")
                    else None
                ),
                partitioned=t["partitioned"],
                format=t.get("format", "PARQUET"),
                has_maps=t.get("has_maps", False),
            )
            for t in data.get("tables", [])
        )

        classifications: dict[str, TierClassification] = {}
        for name, tc_data in data.get("classifications", {}).items():
            classifications[name] = TierClassification(
                table_name=tc_data["table_name"],
                tier=Tier(tc_data["tier"]),
                reason=tc_data["reason"],
                size_bytes=tc_data["size_bytes"],
                last_queried_days_ago=tc_data.get("last_queried_days_ago"),
            )

        return TableInventory(
            inventory_id=data["inventory_id"],
            tables=tables,
            classifications=classifications,
        )

    # ── WriteRepositoryPort ──────────────────────────────────────

    async def save(self, entity: TableInventory) -> None:
        def _save():
            path = self._path_for(entity.inventory_id)
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

    async def get_by_id(self, id: str) -> TableInventory | None:
        def _get():
            path = self._path_for(id)
            if not path.exists():
                return None
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
            return self._deserialize(data)
        return await asyncio.to_thread(_get)

    async def list_all(self) -> list[TableInventory]:
        def _list():
            results: list[TableInventory] = []
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
