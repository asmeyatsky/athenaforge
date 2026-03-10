from __future__ import annotations

from athenaforge.application.commands.transfer.control_streaming_cutover import (
    ControlStreamingCutoverUseCase,
)
from athenaforge.application.commands.transfer.run_dvt_validation import (
    RunDVTValidationUseCase,
)
from athenaforge.application.commands.wave.control_parallel_run import (
    ControlParallelRunUseCase,
)
from athenaforge.application.commands.wave.enforce_wave_gate import (
    EnforceWaveGateUseCase,
)
from athenaforge.application.orchestration.dag_orchestrator import (
    DAGOrchestrator,
    WorkflowStep,
)


class WaveExecutionWorkflow:
    """Build a DAG for executing a single migration wave.

    The per-wave pipeline follows this sequence:

        shadow_run
          -> dvt_shadow
            -> reverse_shadow
              -> dvt_reverse
                -> cutover
                  -> gate_check
    """

    def __init__(
        self,
        parallel_run_uc: ControlParallelRunUseCase,
        dvt_uc: RunDVTValidationUseCase,
        cutover_uc: ControlStreamingCutoverUseCase,
        gate_uc: EnforceWaveGateUseCase,
    ) -> None:
        self._parallel_run_uc = parallel_run_uc
        self._dvt_uc = dvt_uc
        self._cutover_uc = cutover_uc
        self._gate_uc = gate_uc

    def build(
        self,
        wave_id: str,
        *,
        table_pairs: list[tuple[str, str]] | None = None,
        dvt_tier: str = "tier1",
        gate_criteria: dict | None = None,
    ) -> DAGOrchestrator:
        """Create and return a DAG orchestrator for a single wave."""
        _table_pairs = table_pairs if table_pairs is not None else []
        _gate_criteria = gate_criteria if gate_criteria is not None else {
            "dvt_pass_30_days": False,
            "sla_compliance_14_days": False,
            "dashboard_parity": False,
            "zero_athena_queries_14_days": False,
            "bq_spend_within_15pct": False,
            "written_signoff": False,
        }

        orchestrator = DAGOrchestrator()

        # Step 1: shadow_run — start parallel run in shadow mode
        orchestrator.add_step(
            WorkflowStep(
                name="shadow_run",
                execute=lambda wid=wave_id: self._parallel_run_uc.execute(
                    wid, "shadow",
                ),
                depends_on=[],
                is_critical=True,
            )
        )

        # Step 2: dvt_shadow — validate data after shadow run
        orchestrator.add_step(
            WorkflowStep(
                name="dvt_shadow",
                execute=lambda t=dvt_tier, tp=_table_pairs: self._dvt_uc.execute(
                    tier=t,
                    table_pairs=tp,
                ),
                depends_on=["shadow_run"],
                is_critical=True,
            )
        )

        # Step 3: reverse_shadow — switch to reverse-shadow mode
        orchestrator.add_step(
            WorkflowStep(
                name="reverse_shadow",
                execute=lambda wid=wave_id: self._parallel_run_uc.execute(
                    wid, "reverse_shadow",
                ),
                depends_on=["dvt_shadow"],
                is_critical=True,
            )
        )

        # Step 4: dvt_reverse — validate data after reverse shadow
        orchestrator.add_step(
            WorkflowStep(
                name="dvt_reverse",
                execute=lambda t=dvt_tier, tp=_table_pairs: self._dvt_uc.execute(
                    tier=t,
                    table_pairs=tp,
                ),
                depends_on=["reverse_shadow"],
                is_critical=True,
            )
        )

        # Step 5: cutover — perform streaming cutover
        orchestrator.add_step(
            WorkflowStep(
                name="cutover",
                execute=lambda wid=wave_id: self._cutover_uc.execute(
                    job_id=wid,
                    source_topic=f"{wid}-source",
                    target_topic=f"{wid}-target",
                ),
                depends_on=["dvt_reverse"],
                is_critical=True,
            )
        )

        # Step 6: gate_check — enforce quality gate
        orchestrator.add_step(
            WorkflowStep(
                name="gate_check",
                execute=lambda wid=wave_id, gc=_gate_criteria: self._gate_uc.execute(
                    wave_id=wid,
                    criteria=gc,
                ),
                depends_on=["cutover"],
                is_critical=True,
            )
        )

        return orchestrator
