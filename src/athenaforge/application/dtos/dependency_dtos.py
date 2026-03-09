from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class DependencyScanReport:
    spark_jobs: int
    flink_jobs: int
    dags: int
    lambdas: int
    total_references: int
    details: list[dict[str, object]] = field(default_factory=list)


@dataclass(frozen=True)
class DAGRewriteReport:
    dags_processed: int
    dags_rewritten: int
    operators_replaced: int
    changes: list[dict[str, str]] = field(default_factory=list)


@dataclass(frozen=True)
class KafkaMigrationReport:
    topics_migrated: int
    schemas_updated: int
    details: list[dict[str, str]] = field(default_factory=list)


@dataclass(frozen=True)
class LambdaRewriteReport:
    functions_processed: int
    functions_rewritten: int
    details: list[dict[str, str]] = field(default_factory=list)


@dataclass(frozen=True)
class IAMMappingReport:
    policies_mapped: int
    mappings: list[dict[str, str]] = field(default_factory=list)
