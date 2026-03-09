from __future__ import annotations

from athenaforge.application.dtos.dependency_dtos import DAGRewriteReport
from athenaforge.domain.events.dependency_events import DAGRewriteCompleted
from athenaforge.domain.ports.event_bus import EventBusPort
from athenaforge.domain.services.dag_rewriter_service import DAGRewriterService


class RewriteDAGsUseCase:
    """Rewrite Airflow DAGs from AWS operators to GCP equivalents."""

    def __init__(
        self,
        rewriter: DAGRewriterService,
        event_bus: EventBusPort,
    ) -> None:
        self._rewriter = rewriter
        self._event_bus = event_bus

    async def execute(
        self, dag_contents: dict[str, str]
    ) -> DAGRewriteReport:
        dags_processed = 0
        dags_rewritten = 0
        operators_replaced = 0
        changes: list[dict[str, str]] = []

        for file_path, content in dag_contents.items():
            dags_processed += 1
            rewritten, file_changes = self._rewriter.rewrite(content)

            if file_changes:
                dags_rewritten += 1
                operators_replaced += len(file_changes)
                for change in file_changes:
                    changes.append({"file": file_path, "change": change})

        await self._event_bus.publish(
            DAGRewriteCompleted(
                aggregate_id="dag-rewrite",
                dags_rewritten=dags_rewritten,
                operators_replaced=operators_replaced,
            )
        )

        return DAGRewriteReport(
            dags_processed=dags_processed,
            dags_rewritten=dags_rewritten,
            operators_replaced=operators_replaced,
            changes=changes,
        )
