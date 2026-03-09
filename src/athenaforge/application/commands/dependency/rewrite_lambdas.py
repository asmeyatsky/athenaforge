from __future__ import annotations

from athenaforge.application.dtos.dependency_dtos import LambdaRewriteReport
from athenaforge.domain.events.dependency_events import LambdaRewritten
from athenaforge.domain.ports.event_bus import EventBusPort
from athenaforge.domain.services.dependency_scanner import DependencyScanner
from athenaforge.domain.value_objects.dependency_ref import JobType


class RewriteLambdasUseCase:
    """Scan Lambda function sources for Athena references and mark for rewrite."""

    def __init__(
        self,
        scanner: DependencyScanner,
        event_bus: EventBusPort,
    ) -> None:
        self._scanner = scanner
        self._event_bus = event_bus

    async def execute(
        self, lambda_sources: dict[str, str]
    ) -> LambdaRewriteReport:
        functions_processed = 0
        functions_rewritten = 0
        details: list[dict[str, str]] = []

        for function_name, source_code in lambda_sources.items():
            functions_processed += 1
            refs = self._scanner.scan(source_code, function_name)

            # Check if any references point to Athena-related dependencies
            has_athena = any(
                ref.job_type in (JobType.LAMBDA, JobType.AIRFLOW_DAG)
                for ref in refs
            )

            if has_athena:
                functions_rewritten += 1
                details.append(
                    {
                        "function": function_name,
                        "status": "needs_rewrite",
                    }
                )
            else:
                details.append(
                    {
                        "function": function_name,
                        "status": "no_athena_references",
                    }
                )

        await self._event_bus.publish(
            LambdaRewritten(
                aggregate_id="lambda-rewrite",
                functions_rewritten=functions_rewritten,
            )
        )

        return LambdaRewriteReport(
            functions_processed=functions_processed,
            functions_rewritten=functions_rewritten,
            details=details,
        )
