from __future__ import annotations

from dataclasses import dataclass

from athenaforge.domain.events.event_base import DomainEvent


@dataclass(frozen=True)
class ScaffoldGenerated(DomainEvent):
    """Emitted when a line-of-business scaffold has been generated."""

    lob_name: str = ""
    terraform_files: tuple[str, ...] = ()


@dataclass(frozen=True)
class TierClassificationCompleted(DomainEvent):
    """Emitted when table tier classification finishes."""

    total_tables: int = 0
    tier1_count: int = 0
    tier2_count: int = 0
    tier3_count: int = 0


@dataclass(frozen=True)
class DataplexBootstrapCompleted(DomainEvent):
    """Emitted when a Dataplex lake bootstrap completes."""

    lake_name: str = ""
    zones_created: int = 0


@dataclass(frozen=True)
class DeltaLogHealthChecked(DomainEvent):
    """Emitted after a Delta transaction log health check."""

    table_name: str = ""
    log_size_mb: float = 0.0
    status: str = ""
