from __future__ import annotations

from athenaforge.application.dtos.transfer_dtos import CompactionPlan
from athenaforge.domain.events.transfer_events import DeltaCompactionStarted
from athenaforge.domain.ports.cloud_storage_port import CloudStoragePort
from athenaforge.domain.ports.event_bus import EventBusPort
from athenaforge.domain.services.delta_log_health_service import (
    DeltaLogHealthService,
    HealthStatus,
)

_BYTES_PER_MB = 1_048_576


class PlanDeltaCompactionUseCase:
    """Evaluate Delta log health for tables and produce compaction plans."""

    def __init__(
        self,
        health_service: DeltaLogHealthService,
        storage_port: CloudStoragePort,
        event_bus: EventBusPort,
    ) -> None:
        self._health_service = health_service
        self._storage_port = storage_port
        self._event_bus = event_bus

    async def execute(
        self, bucket: str, table_prefixes: list[str]
    ) -> list[CompactionPlan]:
        plans: list[CompactionPlan] = []

        for prefix in table_prefixes:
            log_prefix = f"{prefix}/_delta_log/"
            log_keys = await self._storage_port.list_objects(bucket, log_prefix)

            total_bytes = 0
            for key in log_keys:
                total_bytes += await self._storage_port.get_object_size(bucket, key)

            log_size_mb = total_bytes / _BYTES_PER_MB
            health = self._health_service.check_health(prefix, log_size_mb)

            reduction_pct = _estimate_reduction(health.status)

            plan = CompactionPlan(
                table_name=prefix,
                current_size_bytes=total_bytes,
                estimated_reduction_pct=reduction_pct,
                recommended_action=health.recommendation,
            )
            plans.append(plan)

            await self._event_bus.publish(
                DeltaCompactionStarted(
                    aggregate_id=prefix,
                    table_name=prefix,
                    estimated_reduction_pct=reduction_pct,
                )
            )

        return plans


def _estimate_reduction(status: HealthStatus) -> float:
    """Return an estimated log-size reduction percentage based on health status."""
    if status is HealthStatus.BLOCKED:
        return 80.0
    if status is HealthStatus.CRITICAL:
        return 60.0
    if status is HealthStatus.WARNING:
        return 40.0
    return 0.0
