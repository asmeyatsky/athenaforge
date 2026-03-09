from __future__ import annotations

from athenaforge.application.dtos.sql_dtos import ValidationReport
from athenaforge.domain.events.sql_events import (
    QueryValidationFailed,
    QueryValidationPassed,
)
from athenaforge.domain.ports.bigquery_port import BigQueryPort
from athenaforge.domain.ports.event_bus import EventBusPort


class ValidateQueriesUseCase:
    """Dry-runs translated queries against BigQuery and reports results."""

    def __init__(
        self,
        bigquery_port: BigQueryPort,
        event_bus: EventBusPort,
    ) -> None:
        self._bigquery_port = bigquery_port
        self._event_bus = event_bus

    async def execute(
        self,
        query_paths: list[str],
        query_contents: dict[str, str],
    ) -> ValidationReport:
        """Validate each query via BigQuery dry-run.

        For every path in *query_paths*, looks up the SQL text in
        *query_contents*, performs a dry-run, and publishes pass/fail events.
        """
        passed = 0
        failed = 0
        total_bytes = 0
        failures: list[dict[str, str]] = []

        for query_path in query_paths:
            sql = query_contents.get(query_path, "")
            try:
                bytes_scanned = await self._bigquery_port.dry_run(sql)
                total_bytes += bytes_scanned
                passed += 1
                await self._event_bus.publish(
                    QueryValidationPassed(
                        aggregate_id=query_path,
                        query_path=query_path,
                        dry_run_bytes=bytes_scanned,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                failed += 1
                error_message = str(exc)
                failures.append(
                    {"query_path": query_path, "error": error_message}
                )
                await self._event_bus.publish(
                    QueryValidationFailed(
                        aggregate_id=query_path,
                        query_path=query_path,
                        error_message=error_message,
                    )
                )

        return ValidationReport(
            total_queries=len(query_paths),
            passed=passed,
            failed=failed,
            total_bytes_scanned=total_bytes,
            failures=failures,
        )
