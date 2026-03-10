from __future__ import annotations

from athenaforge.application.dtos.wave_dtos import DashboardMigrationResult


class MigrateDashboardsUseCase:
    """Migrate dashboards from the legacy platform (placeholder implementation)."""

    async def execute(
        self,
        dashboard_configs: list[dict[str, str]],
    ) -> DashboardMigrationResult:
        migrated = 0
        failed = 0
        details: list[dict[str, str]] = []

        for config in dashboard_configs:
            dashboard_name = config.get("name", "unknown")
            # Placeholder: mark each dashboard as successfully migrated
            migrated += 1
            details.append(
                {
                    "name": dashboard_name,
                    "status": "migrated",
                }
            )

        return DashboardMigrationResult(
            dashboards_migrated=migrated,
            dashboards_failed=failed,
            details=details,
        )
