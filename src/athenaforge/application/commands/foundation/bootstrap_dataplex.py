from __future__ import annotations

from athenaforge.application.dtos.foundation_dtos import DataplexBootstrapResult
from athenaforge.domain.events.foundation_events import DataplexBootstrapCompleted
from athenaforge.domain.ports.event_bus import EventBusPort


class BootstrapDataplexUseCase:
    """Bootstrap a Dataplex lake with the specified zones (placeholder)."""

    def __init__(self, event_bus: EventBusPort) -> None:
        self._event_bus = event_bus

    async def execute(
        self, lake_name: str, zones: list[str]
    ) -> DataplexBootstrapResult:
        # Placeholder implementation
        await self._event_bus.publish(
            DataplexBootstrapCompleted(
                aggregate_id=lake_name,
                lake_name=lake_name,
                zones_created=len(zones),
            )
        )

        return DataplexBootstrapResult(
            lakes_created=[lake_name],
            zones_per_lake=len(zones),
            dlp_scans_scheduled=0,
            policy_tags_applied=0,
        )
