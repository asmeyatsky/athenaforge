from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class CompactionPlan:
    table_name: str
    current_size_bytes: int
    estimated_reduction_pct: float
    recommended_action: str


@dataclass(frozen=True)
class EgressCostReport:
    total_size_bytes: int
    scenario_base_usd: float
    scenario_with_credits_usd: float
    scenario_optimized_usd: float
    credit_percentage: float


@dataclass(frozen=True)
class STSJobResult:
    job_id: str
    source_bucket: str
    dest_bucket: str
    status: str


@dataclass(frozen=True)
class DVTValidationReport:
    tier: str
    tables_validated: int
    tables_passed: int
    tables_failed: int
    details: list[dict[str, str]] = field(default_factory=list)


@dataclass(frozen=True)
class StreamingCutoverResult:
    job_id: str
    source_topic: str
    target_topic: str
    status: str
    lag_at_cutover: int
