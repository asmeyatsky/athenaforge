from __future__ import annotations

from dataclasses import dataclass

from athenaforge.domain.events.event_base import DomainEvent


@dataclass(frozen=True)
class DependencyScanCompleted(DomainEvent):
    """Emitted when a full dependency scan finishes."""

    spark_jobs: int = 0
    flink_jobs: int = 0
    dags: int = 0
    lambdas: int = 0


@dataclass(frozen=True)
class DAGRewriteCompleted(DomainEvent):
    """Emitted when DAG rewriting finishes."""

    dags_rewritten: int = 0
    operators_replaced: int = 0


@dataclass(frozen=True)
class KafkaTopicMigrated(DomainEvent):
    """Emitted when Kafka topic migration completes."""

    topics_migrated: int = 0
    schemas_updated: int = 0


@dataclass(frozen=True)
class LambdaRewritten(DomainEvent):
    """Emitted when Lambda function rewriting completes."""

    functions_rewritten: int = 0


@dataclass(frozen=True)
class IAMMappingGenerated(DomainEvent):
    """Emitted when IAM policy mapping generation completes."""

    policies_mapped: int = 0
