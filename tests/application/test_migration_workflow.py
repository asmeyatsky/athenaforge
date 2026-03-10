"""Application-layer tests for MigrationWorkflow."""

from __future__ import annotations

import pytest

from athenaforge.application.orchestration.dag_orchestrator import (
    DAGOrchestrator,
    WorkflowStep,
)
from athenaforge.application.orchestration.migration_workflow import MigrationWorkflow


# ── Stub use cases ───────────────────────────────────────────────────────────
# Each stub only needs an ``execute`` method; the workflow only passes the method
# reference into WorkflowStep.execute.


class StubUseCase:
    """Generic stub that records calls and returns a canned result."""

    def __init__(self, name: str = "stub") -> None:
        self.name = name
        self.called = False

    async def execute(self, *args: object, **kwargs: object) -> dict[str, str]:
        self.called = True
        return {"status": f"{self.name}_done"}


# ── Tests ────────────────────────────────────────────────────────────────────


_DEFAULT_BUILD_KWARGS: dict[str, object] = {
    "manifest_path": "/fake/manifest.yaml",
    "output_dir": "/fake/output",
    "inventory_id": "inv-001",
    "source_dir": "/fake/sql_source",
    "translated_dir": "/fake/translated",
    "bucket": "test-bucket",
}


def _build_workflow() -> tuple[MigrationWorkflow, dict[str, StubUseCase]]:
    """Create a MigrationWorkflow wired with stub use cases."""
    stubs = {
        "scaffold": StubUseCase("scaffold"),
        "classify": StubUseCase("classify"),
        "translate": StubUseCase("translate"),
        "scan_deps": StubUseCase("scan_deps"),
        "validate": StubUseCase("validate"),
        "rewrite_dags": StubUseCase("rewrite_dags"),
        "plan_waves": StubUseCase("plan_waves"),
    }

    wf = MigrationWorkflow(
        scaffold_uc=stubs["scaffold"],       # type: ignore[arg-type]
        classify_uc=stubs["classify"],        # type: ignore[arg-type]
        translate_uc=stubs["translate"],       # type: ignore[arg-type]
        scan_deps_uc=stubs["scan_deps"],      # type: ignore[arg-type]
        validate_uc=stubs["validate"],         # type: ignore[arg-type]
        rewrite_dags_uc=stubs["rewrite_dags"], # type: ignore[arg-type]
        plan_waves_uc=stubs["plan_waves"],     # type: ignore[arg-type]
    )
    return wf, stubs


def _build_dag(wf: MigrationWorkflow) -> DAGOrchestrator:
    """Helper that calls build() with default test parameters."""
    return wf.build(**_DEFAULT_BUILD_KWARGS)  # type: ignore[arg-type]


def test_build_returns_dag_orchestrator():
    """MigrationWorkflow.build() must return a DAGOrchestrator."""
    wf, _ = _build_workflow()
    dag = _build_dag(wf)
    assert isinstance(dag, DAGOrchestrator)


def test_dag_has_correct_number_of_steps():
    """The migration DAG should contain exactly 9 steps."""
    wf, _ = _build_workflow()
    dag = _build_dag(wf)
    # Access internal steps dict to verify count
    assert len(dag._steps) == 9


def test_dag_contains_expected_step_names():
    """All expected step names must be present."""
    wf, _ = _build_workflow()
    dag = _build_dag(wf)
    expected_names = {
        "scaffold",
        "classify_tiers",
        "translate_sql",
        "scan_dependencies",
        "validate_queries",
        "rewrite_dags",
        "plan_waves",
        "execute_waves",
        "final_report",
    }
    assert set(dag._steps.keys()) == expected_names


def test_scaffold_has_no_dependencies():
    """The scaffold step should have no dependencies (it is the root)."""
    wf, _ = _build_workflow()
    dag = _build_dag(wf)
    scaffold = dag._steps["scaffold"]
    assert scaffold.depends_on == []


def test_classify_depends_on_scaffold():
    """classify_tiers must depend on scaffold."""
    wf, _ = _build_workflow()
    dag = _build_dag(wf)
    classify = dag._steps["classify_tiers"]
    assert classify.depends_on == ["scaffold"]


def test_level3_depends_on_classify():
    """translate_sql and scan_dependencies both depend on classify_tiers."""
    wf, _ = _build_workflow()
    dag = _build_dag(wf)

    translate = dag._steps["translate_sql"]
    scan = dag._steps["scan_dependencies"]

    assert "classify_tiers" in translate.depends_on
    assert "classify_tiers" in scan.depends_on


def test_level4_dependencies():
    """validate_queries depends on translate_sql; rewrite_dags depends on scan_dependencies."""
    wf, _ = _build_workflow()
    dag = _build_dag(wf)

    validate = dag._steps["validate_queries"]
    rewrite = dag._steps["rewrite_dags"]

    assert "translate_sql" in validate.depends_on
    assert "scan_dependencies" in rewrite.depends_on


def test_plan_waves_depends_on_validate_and_rewrite():
    """plan_waves must depend on both validate_queries and rewrite_dags."""
    wf, _ = _build_workflow()
    dag = _build_dag(wf)

    plan = dag._steps["plan_waves"]
    assert "validate_queries" in plan.depends_on
    assert "rewrite_dags" in plan.depends_on


def test_execute_waves_depends_on_plan_waves():
    """execute_waves must depend on plan_waves."""
    wf, _ = _build_workflow()
    dag = _build_dag(wf)

    execute = dag._steps["execute_waves"]
    assert "plan_waves" in execute.depends_on


def test_final_report_depends_on_execute_waves():
    """final_report must depend on execute_waves."""
    wf, _ = _build_workflow()
    dag = _build_dag(wf)

    report = dag._steps["final_report"]
    assert "execute_waves" in report.depends_on


def test_all_steps_are_critical():
    """Every step in the migration workflow should be marked as critical."""
    wf, _ = _build_workflow()
    dag = _build_dag(wf)

    for name, step in dag._steps.items():
        assert step.is_critical is True, f"Step '{name}' should be critical"


def test_dag_is_acyclic():
    """The generated DAG must be valid (no cycles) -- verified by building the graph."""
    wf, _ = _build_workflow()
    dag = _build_dag(wf)
    # _build_graph raises ValueError if there's a cycle
    graph = dag._build_graph()
    assert graph.number_of_nodes() == 9


async def test_dag_executes_end_to_end():
    """The full DAG should execute successfully with stub use cases."""
    wf, _ = _build_workflow()
    dag = _build_dag(wf)
    results = await dag.execute()

    assert len(results) == 9
    assert all(r.success for r in results), [
        (r.step_name, r.error) for r in results if not r.success
    ]
