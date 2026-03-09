from __future__ import annotations

from dataclasses import dataclass

from athenaforge.domain.events.event_base import DomainEvent


@dataclass(frozen=True)
class TranslationBatchStarted(DomainEvent):
    """Emitted when a SQL translation batch begins processing."""

    batch_id: str = ""
    file_count: int = 0


@dataclass(frozen=True)
class TranslationBatchCompleted(DomainEvent):
    """Emitted when a SQL translation batch finishes."""

    batch_id: str = ""
    succeeded: int = 0
    failed: int = 0


@dataclass(frozen=True)
class QueryValidationPassed(DomainEvent):
    """Emitted when a translated query passes dry-run validation."""

    query_path: str = ""
    dry_run_bytes: int = 0


@dataclass(frozen=True)
class QueryValidationFailed(DomainEvent):
    """Emitted when a translated query fails validation."""

    query_path: str = ""
    error_message: str = ""


@dataclass(frozen=True)
class MapCascadeAnalysed(DomainEvent):
    """Emitted after map-cascade analysis completes."""

    total_maps: int = 0
    cascade_depth: int = 0
