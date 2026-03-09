from __future__ import annotations

from athenaforge.application.dtos.wave_dtos import KPIReconciliationResult
from athenaforge.domain.ports.event_bus import EventBusPort


class ReconcileKPIsUseCase:
    """Reconcile KPIs between legacy and new platform (placeholder implementation)."""

    def __init__(self, event_bus: EventBusPort) -> None:
        self._event_bus = event_bus

    async def execute(
        self,
        kpi_definitions: list[dict[str, str]],
    ) -> KPIReconciliationResult:
        matched = 0
        mismatched = 0
        details: list[dict[str, str]] = []

        for kpi in kpi_definitions:
            kpi_name = kpi.get("name", "unknown")
            # Placeholder: mark each KPI as matched
            matched += 1
            details.append(
                {
                    "name": kpi_name,
                    "status": "matched",
                }
            )

        return KPIReconciliationResult(
            total_kpis=len(kpi_definitions),
            matched=matched,
            mismatched=mismatched,
            details=details,
        )
