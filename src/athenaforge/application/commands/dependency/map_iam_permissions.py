from __future__ import annotations

from athenaforge.application.dtos.dependency_dtos import IAMMappingReport
from athenaforge.domain.events.dependency_events import IAMMappingGenerated
from athenaforge.domain.ports.event_bus import EventBusPort

# Lake Formation permission → BigQuery IAM role mapping
_PERMISSION_MAP: dict[str, str] = {
    "SELECT": "roles/bigquery.dataViewer",
    "INSERT": "roles/bigquery.dataEditor",
    "UPDATE": "roles/bigquery.dataEditor",
    "DELETE": "roles/bigquery.dataEditor",
    "ALL": "roles/bigquery.dataOwner",
}


class MapIAMPermissionsUseCase:
    """Map Lake Formation policies to BigQuery IAM role equivalents."""

    def __init__(self, event_bus: EventBusPort) -> None:
        self._event_bus = event_bus

    async def execute(
        self, lake_formation_policies: list[dict[str, str]]
    ) -> IAMMappingReport:
        policies_mapped = 0
        mappings: list[dict[str, str]] = []

        for policy in lake_formation_policies:
            permission = policy.get("permission", "").upper()
            resource = policy.get("resource", "unknown")
            principal = policy.get("principal", "unknown")

            bq_role = _PERMISSION_MAP.get(permission, "roles/bigquery.dataViewer")
            policies_mapped += 1

            mappings.append(
                {
                    "lake_formation_permission": permission,
                    "resource": resource,
                    "principal": principal,
                    "bigquery_role": bq_role,
                }
            )

        await self._event_bus.publish(
            IAMMappingGenerated(
                aggregate_id="iam-mapping",
                policies_mapped=policies_mapped,
            )
        )

        return IAMMappingReport(
            policies_mapped=policies_mapped,
            mappings=mappings,
        )
