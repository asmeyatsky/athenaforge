"""Application-layer tests for WaveExecutionWorkflow."""

from __future__ import annotations

import pytest

from athenaforge.application.orchestration.dag_orchestrator import (
    DAGOrchestrator,
    WorkflowStep,
)
from athenaforge.application.orchestration.wave_execution_workflow import (
    WaveExecutionWorkflow,
)


# ── Stub use cases ───────────────────────────────────────────────────────────


class StubControlParallelRunUseCase:
    """Stub for ControlParallelRunUseCase."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def execute(self, wave_id: str, mode: str) -> dict[str, str]:
        self.calls.append((wave_id, mode))
        return {"wave_id": wave_id, "mode": mode}


class StubRunDVTValidationUseCase:
    """Stub for RunDVTValidationUseCase."""

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def execute(
        self,
        tier: str = "tier1",
        table_pairs: list[tuple[str, str]] | None = None,
        primary_keys: dict[str, list[str]] | None = None,
    ) -> dict[str, object]:
        self.calls.append({"tier": tier, "table_pairs": table_pairs or []})
        return {"tier": tier, "status": "passed"}


class StubControlStreamingCutoverUseCase:
    """Stub for ControlStreamingCutoverUseCase."""

    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    async def execute(
        self,
        job_id: str = "",
        source_topic: str = "",
        target_topic: str = "",
        current_lag: int = 0,
    ) -> dict[str, str]:
        self.calls.append(
            {
                "job_id": job_id,
                "source_topic": source_topic,
                "target_topic": target_topic,
            }
        )
        return {"job_id": job_id, "status": "cutover_complete"}


class StubEnforceWaveGateUseCase:
    """Stub for EnforceWaveGateUseCase."""

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def execute(
        self,
        wave_id: str = "",
        criteria: dict[str, bool] | None = None,
    ) -> dict[str, object]:
        self.calls.append({"wave_id": wave_id, "criteria": criteria or {}})
        return {"wave_id": wave_id, "passed": True}


# ── Helpers ──────────────────────────────────────────────────────────────────


def _build_workflow() -> tuple[
    WaveExecutionWorkflow,
    StubControlParallelRunUseCase,
    StubRunDVTValidationUseCase,
    StubControlStreamingCutoverUseCase,
    StubEnforceWaveGateUseCase,
]:
    parallel_run = StubControlParallelRunUseCase()
    dvt = StubRunDVTValidationUseCase()
    cutover = StubControlStreamingCutoverUseCase()
    gate = StubEnforceWaveGateUseCase()

    wf = WaveExecutionWorkflow(
        parallel_run_uc=parallel_run,   # type: ignore[arg-type]
        dvt_uc=dvt,                     # type: ignore[arg-type]
        cutover_uc=cutover,             # type: ignore[arg-type]
        gate_uc=gate,                   # type: ignore[arg-type]
    )
    return wf, parallel_run, dvt, cutover, gate


EXPECTED_STEP_ORDER = [
    "shadow_run",
    "dvt_shadow",
    "reverse_shadow",
    "dvt_reverse",
    "cutover",
    "gate_check",
]


# ── Tests ────────────────────────────────────────────────────────────────────


def test_build_returns_dag_orchestrator():
    """WaveExecutionWorkflow.build() must return a DAGOrchestrator."""
    wf, *_ = _build_workflow()
    dag = wf.build(wave_id="wave-1")
    assert isinstance(dag, DAGOrchestrator)


def test_dag_has_correct_number_of_steps():
    """The wave DAG should contain exactly 6 steps."""
    wf, *_ = _build_workflow()
    dag = wf.build(wave_id="wave-1")
    assert len(dag._steps) == 6


def test_dag_contains_expected_step_names():
    """All expected step names must be present."""
    wf, *_ = _build_workflow()
    dag = wf.build(wave_id="wave-1")
    assert set(dag._steps.keys()) == set(EXPECTED_STEP_ORDER)


def test_sequential_chain_dependencies():
    """Each step should depend on exactly the previous step in the chain."""
    wf, *_ = _build_workflow()
    dag = wf.build(wave_id="wave-1")

    # shadow_run has no dependencies
    assert dag._steps["shadow_run"].depends_on == []

    # Each subsequent step depends on its predecessor
    for i in range(1, len(EXPECTED_STEP_ORDER)):
        current = EXPECTED_STEP_ORDER[i]
        previous = EXPECTED_STEP_ORDER[i - 1]
        step = dag._steps[current]
        assert step.depends_on == [previous], (
            f"Step '{current}' should depend on '{previous}', "
            f"but depends_on={step.depends_on}"
        )


def test_all_steps_are_critical():
    """Every step in the wave execution workflow should be critical."""
    wf, *_ = _build_workflow()
    dag = wf.build(wave_id="wave-1")

    for name, step in dag._steps.items():
        assert step.is_critical is True, f"Step '{name}' should be critical"


def test_dag_is_acyclic():
    """The generated DAG must pass cycle detection."""
    wf, *_ = _build_workflow()
    dag = wf.build(wave_id="wave-1")
    graph = dag._build_graph()
    assert graph.number_of_nodes() == 6


async def test_dag_executes_in_correct_order():
    """Executing the DAG should run steps in the correct sequential order."""
    wf, parallel_run, dvt, cutover, gate = _build_workflow()
    dag = wf.build(wave_id="wave-1")

    # Track execution order using a shared list
    execution_order: list[str] = []
    original_steps = dict(dag._steps)

    for step_name, step in original_steps.items():
        original_fn = step.execute

        # Create closure capturing the correct step_name
        def _make_tracked(name: str, fn: object):  # noqa: ANN001
            async def _tracked() -> object:
                execution_order.append(name)
                return await fn()
            return _tracked

        step.execute = _make_tracked(step_name, original_fn)

    results = await dag.execute()

    assert all(r.success for r in results), [
        (r.step_name, r.error) for r in results if not r.success
    ]
    assert execution_order == EXPECTED_STEP_ORDER


async def test_dag_executes_successfully():
    """The full wave DAG should execute without errors."""
    wf, *_ = _build_workflow()
    dag = wf.build(wave_id="wave-42")
    results = await dag.execute()

    assert len(results) == 6
    assert all(r.success for r in results)


def test_wave_id_passed_to_build():
    """Building with different wave IDs should produce separate DAGs."""
    wf, *_ = _build_workflow()
    dag1 = wf.build(wave_id="wave-A")
    dag2 = wf.build(wave_id="wave-B")

    # Both should have 6 steps; they are independent instances
    assert len(dag1._steps) == 6
    assert len(dag2._steps) == 6
    assert dag1 is not dag2


def test_step_order_matches_docstring_chain():
    """Verify the dependency chain matches: shadow_run -> dvt_shadow -> reverse_shadow -> dvt_reverse -> cutover -> gate_check."""
    wf, *_ = _build_workflow()
    dag = wf.build(wave_id="wave-1")

    # Walk the chain from gate_check back to shadow_run
    chain: list[str] = []
    current = "gate_check"
    while current:
        chain.append(current)
        deps = dag._steps[current].depends_on
        current = deps[0] if deps else ""

    chain.reverse()
    assert chain == EXPECTED_STEP_ORDER
