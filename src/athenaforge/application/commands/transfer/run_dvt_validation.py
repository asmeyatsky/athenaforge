from __future__ import annotations

from athenaforge.application.dtos.transfer_dtos import DVTValidationReport
from athenaforge.domain.events.transfer_events import DVTValidationCompleted
from athenaforge.domain.ports.dvt_port import DVTPort
from athenaforge.domain.ports.event_bus import EventBusPort


class RunDVTValidationUseCase:
    """Run Data Validation Tool checks at the specified tier level."""

    def __init__(
        self,
        dvt_port: DVTPort,
        event_bus: EventBusPort,
    ) -> None:
        self._dvt_port = dvt_port
        self._event_bus = event_bus

    async def execute(
        self,
        tier: str,
        table_pairs: list[tuple[str, str]],
        primary_keys: dict[str, list[str]] | None = None,
    ) -> DVTValidationReport:
        details: list[dict[str, str]] = []
        tables_passed = 0
        tables_failed = 0

        for source_table, target_table in table_pairs:
            table_result = await self._validate_table(
                tier, source_table, target_table, primary_keys
            )
            if table_result["status"] == "passed":
                tables_passed += 1
            else:
                tables_failed += 1
            details.append(table_result)

        report = DVTValidationReport(
            tier=tier,
            tables_validated=len(table_pairs),
            tables_passed=tables_passed,
            tables_failed=tables_failed,
            details=details,
        )

        await self._event_bus.publish(
            DVTValidationCompleted(
                aggregate_id=tier,
                tier=tier,
                tables_validated=report.tables_validated,
                tables_passed=report.tables_passed,
                tables_failed=report.tables_failed,
            )
        )

        return report

    async def _validate_table(
        self,
        tier: str,
        source_table: str,
        target_table: str,
        primary_keys: dict[str, list[str]] | None,
    ) -> dict[str, str]:
        """Run the appropriate validation checks for the given tier."""
        checks_passed: list[str] = []
        checks_failed: list[str] = []

        # All tiers: row count
        row_count = await self._dvt_port.validate_row_count(
            source_table, target_table
        )
        if row_count.get("status") == "pass":
            checks_passed.append("row_count")
        else:
            checks_failed.append("row_count")

        # Tier 1 and Tier 2: column aggregates
        if tier in ("tier1", "tier2"):
            col_agg = await self._dvt_port.validate_column_aggregates(
                source_table, target_table, columns=[]
            )
            if col_agg.get("status") == "pass":
                checks_passed.append("column_aggregates")
            else:
                checks_failed.append("column_aggregates")

        # Tier 1 only: row hash
        if tier == "tier1":
            pks = (primary_keys or {}).get(source_table, [])
            row_hash = await self._dvt_port.validate_row_hash(
                source_table, target_table, primary_keys=pks
            )
            if row_hash.get("status") == "pass":
                checks_passed.append("row_hash")
            else:
                checks_failed.append("row_hash")

        status = "passed" if not checks_failed else "failed"
        return {
            "source_table": source_table,
            "target_table": target_table,
            "status": status,
            "checks_passed": ",".join(checks_passed),
            "checks_failed": ",".join(checks_failed),
        }
