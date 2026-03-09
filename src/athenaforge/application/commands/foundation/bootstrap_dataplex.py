from __future__ import annotations

from athenaforge.domain.events.foundation_events import DataplexBootstrapCompleted
from athenaforge.domain.ports.event_bus import EventBusPort


class BootstrapDataplexUseCase:
    """Bootstrap a Dataplex lake with the specified zones (placeholder)."""

    def __init__(self, event_bus: EventBusPort) -> None:
        self._event_bus = event_bus

    async def execute(
        self, lake_name: str, zones: list[str]
    ) -> dict:
        # Placeholder implementation
        result: dict = {
            "lake_name": lake_name,
            "zones_created": zones,
            "status": "completed",
        }

        await self._event_bus.publish(
            DataplexBootstrapCompleted(
                aggregate_id=lake_name,
                lake_name=lake_name,
                zones_created=len(zones),
            )
        )

        return result
