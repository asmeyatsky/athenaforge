from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ScaffoldResult:
    lob_name: str
    terraform_files: list[str]
    output_dir: str


@dataclass(frozen=True)
class ClassificationResult:
    total_tables: int
    tier1_count: int
    tier2_count: int
    tier3_count: int
    classifications: dict[str, str]  # table_name -> tier


@dataclass(frozen=True)
class PricingResult:
    edition: str
    slots: int
    monthly_cost_usd: float
    terraform_file: str | None = None


@dataclass(frozen=True)
class DeltaHealthResult:
    table_name: str
    log_size_mb: float
    status: str
    recommendation: str


@dataclass(frozen=True)
class TierSummaryDTO:
    total_tables: int
    tier1_count: int
    tier1_size_bytes: int
    tier2_count: int
    tier2_size_bytes: int
    tier3_count: int
    tier3_size_bytes: int


@dataclass(frozen=True)
class DataplexBootstrapResult:
    lakes_created: list[str]
    zones_per_lake: int
    dlp_scans_scheduled: int
    policy_tags_applied: int
