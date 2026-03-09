from __future__ import annotations

from athenaforge.application.dtos.foundation_dtos import TierSummaryDTO
from athenaforge.domain.entities.table_inventory import TableInventory
from athenaforge.domain.ports.repository_ports import ReadRepositoryPort
from athenaforge.domain.value_objects.tier import Tier


class GetTierSummaryQuery:
    """Query that returns an aggregated tier summary for a table inventory."""

    def __init__(
        self, table_repo: ReadRepositoryPort[TableInventory]
    ) -> None:
        self._table_repo = table_repo

    async def execute(self, inventory_id: str) -> TierSummaryDTO:
        inventory = await self._table_repo.get_by_id(inventory_id)
        if inventory is None:
            raise ValueError(f"Inventory '{inventory_id}' not found")

        tier_counts = {Tier.TIER_1: 0, Tier.TIER_2: 0, Tier.TIER_3: 0}
        tier_sizes = {Tier.TIER_1: 0, Tier.TIER_2: 0, Tier.TIER_3: 0}

        for table in inventory.tables:
            classification = inventory.classifications.get(table.table_name)
            if classification is None:
                continue
            tier_counts[classification.tier] += 1
            tier_sizes[classification.tier] += table.size_bytes

        return TierSummaryDTO(
            total_tables=len(inventory.tables),
            tier1_count=tier_counts[Tier.TIER_1],
            tier1_size_bytes=tier_sizes[Tier.TIER_1],
            tier2_count=tier_counts[Tier.TIER_2],
            tier2_size_bytes=tier_sizes[Tier.TIER_2],
            tier3_count=tier_counts[Tier.TIER_3],
            tier3_size_bytes=tier_sizes[Tier.TIER_3],
        )
