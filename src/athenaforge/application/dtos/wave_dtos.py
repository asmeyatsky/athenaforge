from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class WavePlanResult:
    total_waves: int
    total_tables: int
    waves: list[dict[str, object]]  # wave_id, name, lob, table_count, estimated_days


@dataclass(frozen=True)
class ParallelRunResult:
    wave_id: str
    previous_mode: str
    current_mode: str
    success: bool


@dataclass(frozen=True)
class RollbackCheckResult:
    should_rollback: bool
    conditions: list[dict[str, object]]  # name, triggered, details


@dataclass(frozen=True)
class WaveGateResult:
    wave_id: str
    passed: bool
    criteria_met: list[str] = field(default_factory=list)
    criteria_failed: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class DashboardMigrationResult:
    dashboards_migrated: int
    dashboards_failed: int
    details: list[dict[str, str]] = field(default_factory=list)


@dataclass(frozen=True)
class KPIReconciliationResult:
    total_kpis: int
    matched: int
    mismatched: int
    details: list[dict[str, str]] = field(default_factory=list)
