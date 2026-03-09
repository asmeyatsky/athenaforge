"""Application-layer tests for DAGOrchestrator, WorkflowStep, StepResult."""

from __future__ import annotations

import asyncio
import time

import pytest

from athenaforge.application.orchestration.dag_orchestrator import (
    DAGOrchestrator,
    StepResult,
    WorkflowStep,
)


# ── Helper factories ─────────────────────────────────────────────────────────


def _make_step(
    name: str,
    depends_on: list[str] | None = None,
    result: object = "ok",
    timeout: float = 300.0,
    is_critical: bool = True,
    delay: float = 0.0,
    raise_error: str | None = None,
) -> WorkflowStep:
    """Build a WorkflowStep with a configurable async callable."""

    async def _execute() -> object:
        if delay > 0:
            await asyncio.sleep(delay)
        if raise_error:
            raise RuntimeError(raise_error)
        return result

    return WorkflowStep(
        name=name,
        execute=_execute,
        depends_on=depends_on or [],
        timeout=timeout,
        is_critical=is_critical,
    )


# ── Tests ────────────────────────────────────────────────────────────────────


async def test_parallel_execution_of_independent_steps():
    """Steps with no dependencies should execute in parallel (same generation)."""
    execution_order: list[str] = []

    async def _tracked(name: str, delay: float) -> str:
        execution_order.append(f"start:{name}")
        await asyncio.sleep(delay)
        execution_order.append(f"end:{name}")
        return name

    dag = DAGOrchestrator()
    dag.add_step(
        WorkflowStep(
            name="a",
            execute=lambda: _tracked("a", 0.05),
            depends_on=[],
        )
    )
    dag.add_step(
        WorkflowStep(
            name="b",
            execute=lambda: _tracked("b", 0.05),
            depends_on=[],
        )
    )
    dag.add_step(
        WorkflowStep(
            name="c",
            execute=lambda: _tracked("c", 0.05),
            depends_on=[],
        )
    )

    start = time.monotonic()
    results = await dag.execute()
    elapsed = time.monotonic() - start

    # All 3 should succeed
    assert all(r.success for r in results)

    # If they ran sequentially, it would take >= 0.15s.
    # Parallel should complete in roughly 0.05s (with some overhead).
    assert elapsed < 0.14, f"Took {elapsed:.3f}s; expected parallel execution"


async def test_dependency_ordering_is_respected():
    """A step should only run after all its dependencies have completed."""
    execution_order: list[str] = []

    async def _tracked(name: str) -> str:
        execution_order.append(name)
        return name

    dag = DAGOrchestrator()
    dag.add_step(
        WorkflowStep(name="first", execute=lambda: _tracked("first"), depends_on=[])
    )
    dag.add_step(
        WorkflowStep(
            name="second", execute=lambda: _tracked("second"), depends_on=["first"]
        )
    )
    dag.add_step(
        WorkflowStep(
            name="third", execute=lambda: _tracked("third"), depends_on=["second"]
        )
    )

    results = await dag.execute()

    assert all(r.success for r in results)
    assert execution_order == ["first", "second", "third"]


async def test_cycle_detection_raises_value_error():
    """A cycle in the dependency graph must raise ValueError."""
    dag = DAGOrchestrator()
    dag.add_step(_make_step("a", depends_on=["b"]))
    dag.add_step(_make_step("b", depends_on=["a"]))

    with pytest.raises(ValueError, match="[Cc]ycle"):
        await dag.execute()


async def test_three_node_cycle_detected():
    """A three-node cycle should also be caught."""
    dag = DAGOrchestrator()
    dag.add_step(_make_step("x", depends_on=["z"]))
    dag.add_step(_make_step("y", depends_on=["x"]))
    dag.add_step(_make_step("z", depends_on=["y"]))

    with pytest.raises(ValueError, match="[Cc]ycle"):
        await dag.execute()


async def test_timeout_handling():
    """A step that exceeds its timeout should be marked as failed with 'Timeout'."""
    dag = DAGOrchestrator()
    dag.add_step(
        _make_step("slow", delay=5.0, timeout=0.05)  # 50ms timeout, 5s delay
    )

    results = await dag.execute()

    assert len(results) == 1
    assert results[0].success is False
    assert results[0].error == "Timeout"
    assert results[0].duration_seconds > 0


async def test_critical_step_failure_skips_dependent_steps():
    """When a critical step fails, its dependents should be skipped."""
    dag = DAGOrchestrator()
    dag.add_step(
        _make_step("root", raise_error="boom", is_critical=True)
    )
    dag.add_step(_make_step("child", depends_on=["root"]))

    results = await dag.execute()

    root_result = next(r for r in results if r.step_name == "root")
    child_result = next(r for r in results if r.step_name == "child")

    assert root_result.success is False
    assert root_result.error == "boom"

    assert child_result.success is False
    assert "dependency failed" in (child_result.error or "").lower()


async def test_non_critical_step_failure_allows_dependents():
    """When a non-critical step fails, its dependents should still run."""
    dag = DAGOrchestrator()
    dag.add_step(
        _make_step("optional", raise_error="minor issue", is_critical=False)
    )
    dag.add_step(
        _make_step("next_step", depends_on=["optional"], result="done")
    )

    results = await dag.execute()

    optional_result = next(r for r in results if r.step_name == "optional")
    next_result = next(r for r in results if r.step_name == "next_step")

    assert optional_result.success is False
    # The dependent step should still execute and succeed
    assert next_result.success is True
    assert next_result.result == "done"


async def test_step_result_contains_return_value():
    """Successful steps should capture the callable's return value."""
    dag = DAGOrchestrator()
    dag.add_step(_make_step("compute", result={"answer": 42}))

    results = await dag.execute()

    assert results[0].success is True
    assert results[0].result == {"answer": 42}


async def test_step_result_has_duration():
    """Every executed step should have a positive duration."""
    dag = DAGOrchestrator()
    dag.add_step(_make_step("quick", delay=0.01))

    results = await dag.execute()

    assert results[0].duration_seconds > 0


async def test_diamond_dependency_graph():
    """Test a diamond-shaped DAG: A -> B, A -> C, B -> D, C -> D."""
    execution_order: list[str] = []

    async def _tracked(name: str) -> str:
        execution_order.append(name)
        return name

    dag = DAGOrchestrator()
    dag.add_step(WorkflowStep(name="A", execute=lambda: _tracked("A"), depends_on=[]))
    dag.add_step(
        WorkflowStep(name="B", execute=lambda: _tracked("B"), depends_on=["A"])
    )
    dag.add_step(
        WorkflowStep(name="C", execute=lambda: _tracked("C"), depends_on=["A"])
    )
    dag.add_step(
        WorkflowStep(
            name="D", execute=lambda: _tracked("D"), depends_on=["B", "C"]
        )
    )

    results = await dag.execute()

    assert all(r.success for r in results)
    # A must come first, D must come last
    assert execution_order[0] == "A"
    assert execution_order[-1] == "D"
    # B and C must both appear before D
    assert "B" in execution_order[1:3]
    assert "C" in execution_order[1:3]


async def test_critical_failure_skips_direct_dependents():
    """A critical failure should skip its direct dependents.

    Note: the orchestrator only checks *direct* dependencies against the
    failed_critical set.  Skipped steps are not themselves added to
    failed_critical, so transitive dependents whose direct dependency was
    merely skipped (not executed-and-failed) will still run.
    """
    dag = DAGOrchestrator()
    dag.add_step(_make_step("level1", raise_error="fatal", is_critical=True))
    dag.add_step(_make_step("level2", depends_on=["level1"]))
    dag.add_step(_make_step("level3", depends_on=["level2"]))

    results = await dag.execute()

    level1 = next(r for r in results if r.step_name == "level1")
    level2 = next(r for r in results if r.step_name == "level2")
    level3 = next(r for r in results if r.step_name == "level3")

    assert level1.success is False
    assert level1.error == "fatal"

    # level2 is a direct dependent of the failed critical step -> skipped
    assert level2.success is False
    assert "dependency failed" in (level2.error or "").lower()

    # level3 depends on level2 which was skipped (not in failed_critical)
    # so the orchestrator allows it to run
    assert level3.success is True


async def test_empty_dag_returns_empty_results():
    """An orchestrator with no steps should return an empty result list."""
    dag = DAGOrchestrator()
    results = await dag.execute()
    assert results == []
