from __future__ import annotations

from athenaforge.application.dtos.wave_dtos import WavePlanResult
from athenaforge.domain.entities.table_inventory import TableInventory
from athenaforge.domain.events.wave_events import WavePlanned
from athenaforge.domain.ports.event_bus import EventBusPort
from athenaforge.domain.ports.repository_ports import ReadRepositoryPort
from athenaforge.domain.services.wave_planner_service import WavePlannerService


class PlanWavesUseCase:
    """Plan migration waves for a given inventory and set of LOBs."""

    def __init__(
        self,
        planner: WavePlannerService,
        table_repo: ReadRepositoryPort[TableInventory],
        event_bus: EventBusPort,
    ) -> None:
        self._planner = planner
        self._table_repo = table_repo
        self._event_bus = event_bus

    async def execute(
        self,
        inventory_id: str,
        lobs: list[str],
        max_parallel: int = 3,
    ) -> WavePlanResult:
        inventory = await self._table_repo.get_by_id(inventory_id)
        if inventory is None:
            raise ValueError(f"Inventory '{inventory_id}' not found")

        classifications = list(inventory.classifications.values())
        wave_definitions = self._planner.plan_waves(
            classifications, lobs, max_parallel=max_parallel,
        )

        total_tables = sum(len(wd.tables) for wd in wave_definitions)

        await self._event_bus.publish(
            WavePlanned(
                aggregate_id=inventory_id,
                wave_count=len(wave_definitions),
                total_tables=total_tables,
            )
        )

        waves: list[dict[str, object]] = [
            {
                "wave_id": wd.wave_id,
                "name": wd.name,
                "lob": wd.lob,
                "table_count": len(wd.tables),
                "estimated_days": wd.estimated_duration_days,
            }
            for wd in wave_definitions
        ]

        return WavePlanResult(
            total_waves=len(wave_definitions),
            total_tables=total_tables,
            waves=waves,
        )
