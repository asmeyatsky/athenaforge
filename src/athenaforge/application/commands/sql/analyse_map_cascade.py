from __future__ import annotations

from athenaforge.application.dtos.sql_dtos import MapCascadeResult
from athenaforge.domain.events.sql_events import MapCascadeAnalysed
from athenaforge.domain.ports.event_bus import EventBusPort
from athenaforge.domain.services.map_cascade_analyser import MapCascadeAnalyser


class AnalyseMapCascadeUseCase:
    """Orchestrates map-cascade dependency analysis and publishes results."""

    def __init__(
        self,
        analyser: MapCascadeAnalyser,
        event_bus: EventBusPort,
    ) -> None:
        self._analyser = analyser
        self._event_bus = event_bus

    async def execute(
        self, dependencies: dict[str, list[str]]
    ) -> MapCascadeResult:
        """Analyse dependency cascades and co-migration batches.

        1. Run ``MapCascadeAnalyser.analyse()`` to compute cascades.
        2. Run ``get_co_migration_batches()`` for connected components.
        3. Publish a ``MapCascadeAnalysed`` event.
        """
        cascades = self._analyser.analyse(dependencies)
        co_migration_batches = self._analyser.get_co_migration_batches(dependencies)

        total_maps = len(cascades)
        cascade_depth = max(
            (c.cascade_depth for c in cascades), default=0
        )

        await self._event_bus.publish(
            MapCascadeAnalysed(
                aggregate_id="map-cascade",
                total_maps=total_maps,
                cascade_depth=cascade_depth,
            )
        )

        return MapCascadeResult(
            total_maps=total_maps,
            cascade_depth=cascade_depth,
            co_migration_batches=co_migration_batches,
        )
