from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TranslationBatchResult:
    batch_id: str
    total_files: int
    succeeded: int
    failed: int
    patterns_applied: list[str]


@dataclass(frozen=True)
class MapCascadeResult:
    total_maps: int
    cascade_depth: int
    co_migration_batches: list[tuple[str, ...]]


@dataclass(frozen=True)
class UDFClassificationReport:
    total_udfs: int
    sql_udfs: int
    js_udfs: int
    cloud_run_udfs: int
    classifications: dict[str, str]  # udf_name -> category


@dataclass(frozen=True)
class ValidationReport:
    total_queries: int
    passed: int
    failed: int
    total_bytes_scanned: int
    failures: list[dict[str, str]]  # list of {query_path, error}
