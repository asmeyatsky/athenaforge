from __future__ import annotations

from athenaforge.application.dtos.foundation_dtos import ClassificationResult
from athenaforge.domain.entities.table_inventory import TableInventory
from athenaforge.domain.events.foundation_events import TierClassificationCompleted
from athenaforge.domain.ports.event_bus import EventBusPort
from athenaforge.domain.ports.repository_ports import ReadRepositoryPort, WriteRepositoryPort
from athenaforge.domain.services.tier_classification_service import (
    TierClassificationService,
)
from athenaforge.domain.value_objects.tier import Tier


class ClassifyTiersUseCase:
    """Classify all tables in an inventory into migration tiers."""

    def __init__(
        self,
        tier_service: TierClassificationService,
        table_repo: ReadRepositoryPort[TableInventory],
        event_bus: EventBusPort,
        table_write_repo: WriteRepositoryPort[TableInventory] | None = None,
    ) -> None:
        self._tier_service = tier_service
        self._table_repo = table_repo
        self._event_bus = event_bus
        self._table_write_repo = table_write_repo

    async def execute(self, inventory_id: str) -> ClassificationResult:
        inventory = await self._table_repo.get_by_id(inventory_id)
        if inventory is None:
            raise ValueError(f"Inventory '{inventory_id}' not found")

        updated_inventory = inventory.classify_all(self._tier_service)

        # Persist the classified inventory so downstream commands can use it
        if self._table_write_repo is not None:
            await self._table_write_repo.save(updated_inventory)

        tier_counts = {Tier.TIER_1: 0, Tier.TIER_2: 0, Tier.TIER_3: 0}
        classification_map: dict[str, str] = {}

        for table_name, tc in updated_inventory.classifications.items():
            tier_counts[tc.tier] += 1
            classification_map[table_name] = tc.tier.value

        await self._event_bus.publish(
            TierClassificationCompleted(
                aggregate_id=inventory_id,
                total_tables=len(updated_inventory.tables),
                tier1_count=tier_counts[Tier.TIER_1],
                tier2_count=tier_counts[Tier.TIER_2],
                tier3_count=tier_counts[Tier.TIER_3],
            )
        )

        return ClassificationResult(
            total_tables=len(updated_inventory.tables),
            tier1_count=tier_counts[Tier.TIER_1],
            tier2_count=tier_counts[Tier.TIER_2],
            tier3_count=tier_counts[Tier.TIER_3],
            classifications=classification_map,
        )
