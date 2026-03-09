from __future__ import annotations

from athenaforge.application.dtos.foundation_dtos import DeltaHealthResult
from athenaforge.domain.events.foundation_events import DeltaLogHealthChecked
from athenaforge.domain.ports.cloud_storage_port import CloudStoragePort
from athenaforge.domain.ports.event_bus import EventBusPort
from athenaforge.domain.services.delta_log_health_service import DeltaLogHealthService

_BYTES_PER_MB = 1_048_576


class CheckDeltaHealthUseCase:
    """Check Delta transaction log health for tables under a bucket prefix."""

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
        self, bucket: str, table_prefix: str
    ) -> list[DeltaHealthResult]:
        objects = await self._storage_port.list_objects(bucket, table_prefix)

        delta_log_objects = [
            obj for obj in objects if "/_delta_log/" in obj or obj.endswith("/_delta_log")
        ]

        # Group by table name (parent of _delta_log)
        table_logs: dict[str, list[str]] = {}
        for obj in delta_log_objects:
            parts = obj.split("/_delta_log/")
            table_name = parts[0].rsplit("/", 1)[-1] if "/" in parts[0] else parts[0]
            table_logs.setdefault(table_name, []).append(obj)

        results: list[DeltaHealthResult] = []

        for table_name, log_keys in table_logs.items():
            total_size = 0
            for key in log_keys:
                total_size += await self._storage_port.get_object_size(bucket, key)

            log_size_mb = total_size / _BYTES_PER_MB
            health = self._health_service.check_health(table_name, log_size_mb)

            await self._event_bus.publish(
                DeltaLogHealthChecked(
                    aggregate_id=table_name,
                    table_name=table_name,
                    log_size_mb=health.log_size_mb,
                    status=health.status.value,
                )
            )

            results.append(
                DeltaHealthResult(
                    table_name=health.table_name,
                    log_size_mb=health.log_size_mb,
                    status=health.status.value,
                    recommendation=health.recommendation,
                )
            )

        return results
