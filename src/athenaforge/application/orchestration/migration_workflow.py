from __future__ import annotations

from athenaforge.application.commands.dependency.rewrite_dags import (
    RewriteDAGsUseCase,
)
from athenaforge.application.commands.dependency.scan_spark_flink_jobs import (
    ScanSparkFlinkJobsUseCase,
)
from athenaforge.application.commands.foundation.classify_tiers import (
    ClassifyTiersUseCase,
)
from athenaforge.application.commands.foundation.generate_scaffold import (
    GenerateScaffoldUseCase,
)
from athenaforge.application.commands.sql.translate_batch import (
    TranslateBatchUseCase,
)
from athenaforge.application.commands.sql.validate_queries import (
    ValidateQueriesUseCase,
)
from athenaforge.application.commands.wave.plan_waves import PlanWavesUseCase
from athenaforge.application.orchestration.dag_orchestrator import (
    DAGOrchestrator,
    WorkflowStep,
)


class MigrationWorkflow:
    """Build a 7-level DAG representing an end-to-end migration workflow.

    Level 1: scaffold
    Level 2: classify_tiers (depends on scaffold)
    Level 3: translate_sql, scan_dependencies (both depend on classify_tiers) — parallel
    Level 4: validate_queries (depends on translate_sql),
             rewrite_dags (depends on scan_dependencies) — parallel
    Level 5: plan_waves (depends on validate_queries, rewrite_dags)
    Level 6: execute_waves (depends on plan_waves) — sequential per wave
    Level 7: final_report (depends on execute_waves)
    """

    def __init__(
        self,
        scaffold_uc: GenerateScaffoldUseCase,
        classify_uc: ClassifyTiersUseCase,
        translate_uc: TranslateBatchUseCase,
        scan_deps_uc: ScanSparkFlinkJobsUseCase,
        validate_uc: ValidateQueriesUseCase,
        rewrite_dags_uc: RewriteDAGsUseCase,
        plan_waves_uc: PlanWavesUseCase,
    ) -> None:
        self._scaffold_uc = scaffold_uc
        self._classify_uc = classify_uc
        self._translate_uc = translate_uc
        self._scan_deps_uc = scan_deps_uc
        self._validate_uc = validate_uc
        self._rewrite_dags_uc = rewrite_dags_uc
        self._plan_waves_uc = plan_waves_uc

    def build(
        self,
        *,
        manifest_path: str,
        output_dir: str,
        inventory_id: str,
        source_dir: str,
        translated_dir: str,
        bucket: str,
        prefixes: list[str] | None = None,
        lobs: list[str] | None = None,
        max_parallel: int = 3,
    ) -> DAGOrchestrator:
        """Create and return a fully-configured DAG orchestrator."""
        _prefixes = prefixes or []
        _lobs = lobs or []

        orchestrator = DAGOrchestrator()

        # Level 1
        orchestrator.add_step(
            WorkflowStep(
                name="scaffold",
                execute=lambda mp=manifest_path, od=output_dir: self._scaffold_uc.execute(mp, od),
                depends_on=[],
                is_critical=True,
            )
        )

        # Level 2
        orchestrator.add_step(
            WorkflowStep(
                name="classify_tiers",
                execute=lambda iid=inventory_id: self._classify_uc.execute(iid),
                depends_on=["scaffold"],
                is_critical=True,
            )
        )

        # Level 3 — parallel
        orchestrator.add_step(
            WorkflowStep(
                name="translate_sql",
                execute=lambda sd=source_dir, od=output_dir: self._translate_uc.execute(sd, od),
                depends_on=["classify_tiers"],
                is_critical=True,
            )
        )
        orchestrator.add_step(
            WorkflowStep(
                name="scan_dependencies",
                execute=lambda b=bucket, p=_prefixes: self._scan_deps_uc.execute(b, p),
                depends_on=["classify_tiers"],
                is_critical=True,
            )
        )

        # Level 4 — parallel
        orchestrator.add_step(
            WorkflowStep(
                name="validate_queries",
                execute=lambda td=translated_dir: self._validate_uc.execute(
                    query_paths=[], query_contents={},
                ),
                depends_on=["translate_sql"],
                is_critical=True,
            )
        )
        orchestrator.add_step(
            WorkflowStep(
                name="rewrite_dags",
                execute=lambda: self._rewrite_dags_uc.execute(dag_contents={}),
                depends_on=["scan_dependencies"],
                is_critical=True,
            )
        )

        # Level 5
        orchestrator.add_step(
            WorkflowStep(
                name="plan_waves",
                execute=lambda iid=inventory_id, lb=_lobs, mp=max_parallel: self._plan_waves_uc.execute(iid, lb, mp),
                depends_on=["validate_queries", "rewrite_dags"],
                is_critical=True,
            )
        )

        # Level 6
        orchestrator.add_step(
            WorkflowStep(
                name="execute_waves",
                execute=self._execute_waves,
                depends_on=["plan_waves"],
                is_critical=True,
            )
        )

        # Level 7
        orchestrator.add_step(
            WorkflowStep(
                name="final_report",
                execute=self._generate_final_report,
                depends_on=["execute_waves"],
                is_critical=True,
            )
        )

        return orchestrator

    async def _execute_waves(self) -> dict[str, object]:
        """Placeholder for sequential per-wave execution.

        In a full implementation this would iterate over the planned waves
        and execute each one using a ``WaveExecutionWorkflow``.
        """
        return {"status": "waves_executed"}

    async def _generate_final_report(self) -> dict[str, object]:
        """Placeholder for final migration report generation."""
        return {"status": "report_generated"}
