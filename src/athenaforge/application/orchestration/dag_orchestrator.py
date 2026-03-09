from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Awaitable

import networkx as nx


@dataclass(frozen=True)
class StepResult:
    step_name: str
    success: bool
    result: Any = None
    error: str | None = None
    duration_seconds: float = 0.0


@dataclass
class WorkflowStep:
    name: str
    execute: Callable[..., Awaitable[Any]]
    depends_on: list[str] = field(default_factory=list)
    timeout: float = 300.0  # seconds
    is_critical: bool = True


class DAGOrchestrator:
    def __init__(self) -> None:
        self._steps: dict[str, WorkflowStep] = {}

    def add_step(self, step: WorkflowStep) -> None:
        self._steps[step.name] = step

    def _build_graph(self) -> nx.DiGraph:
        graph = nx.DiGraph()
        for name, step in self._steps.items():
            graph.add_node(name)
            for dep in step.depends_on:
                graph.add_edge(dep, name)
        if not nx.is_directed_acyclic_graph(graph):
            raise ValueError("Workflow contains cycles")
        return graph

    async def execute(self) -> list[StepResult]:
        graph = self._build_graph()
        results: dict[str, StepResult] = {}
        completed: set[str] = set()
        failed_critical: set[str] = set()

        # Process steps in topological generations (parallel within each generation)
        for generation in nx.topological_generations(graph):
            # Filter out steps whose critical dependencies failed
            runnable: list[WorkflowStep] = []
            for step_name in generation:
                step = self._steps[step_name]
                deps_failed = any(d in failed_critical for d in step.depends_on)
                if deps_failed:
                    results[step_name] = StepResult(
                        step_name=step_name,
                        success=False,
                        error="Skipped: dependency failed",
                    )
                else:
                    runnable.append(step)

            # Execute all runnable steps in this generation in parallel
            if runnable:
                step_results = await asyncio.gather(
                    *[self._execute_step(step) for step in runnable],
                    return_exceptions=False,
                )
                for step, result in zip(runnable, step_results):
                    results[step.name] = result
                    if result.success:
                        completed.add(step.name)
                    elif step.is_critical:
                        failed_critical.add(step.name)

        return [results[name] for name in self._steps]

    async def _execute_step(self, step: WorkflowStep) -> StepResult:
        start = time.monotonic()
        try:
            result = await asyncio.wait_for(step.execute(), timeout=step.timeout)
            duration = time.monotonic() - start
            return StepResult(
                step_name=step.name,
                success=True,
                result=result,
                duration_seconds=duration,
            )
        except asyncio.TimeoutError:
            duration = time.monotonic() - start
            return StepResult(
                step_name=step.name,
                success=False,
                error="Timeout",
                duration_seconds=duration,
            )
        except Exception as e:
            duration = time.monotonic() - start
            return StepResult(
                step_name=step.name,
                success=False,
                error=str(e),
                duration_seconds=duration,
            )
