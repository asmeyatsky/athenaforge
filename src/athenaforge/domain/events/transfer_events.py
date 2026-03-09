from __future__ import annotations

from dataclasses import dataclass

from athenaforge.domain.events.event_base import DomainEvent


@dataclass(frozen=True)
class DeltaCompactionStarted(DomainEvent):
    """Emitted when Delta table compaction begins."""

    table_name: str = ""
    estimated_reduction_pct: float = 0.0


@dataclass(frozen=True)
class TransferJobCreated(DomainEvent):
    """Emitted when a data transfer job is created."""

    job_id: str = ""
    source_bucket: str = ""
    dest_bucket: str = ""
    size_bytes: int = 0


@dataclass(frozen=True)
class TransferJobCompleted(DomainEvent):
    """Emitted when a data transfer job finishes."""

    job_id: str = ""
    bytes_transferred: int = 0
    duration_seconds: float = 0.0


@dataclass(frozen=True)
class DVTValidationCompleted(DomainEvent):
    """Emitted when DVT validation for a tier completes."""

    tier: str = ""
    tables_validated: int = 0
    tables_passed: int = 0
    tables_failed: int = 0


@dataclass(frozen=True)
class StreamingCutoverInitiated(DomainEvent):
    """Emitted when a streaming cutover is initiated."""

    job_id: str = ""
    source_topic: str = ""
    target_topic: str = ""
