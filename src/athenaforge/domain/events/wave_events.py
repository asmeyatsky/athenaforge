from __future__ import annotations

from dataclasses import dataclass

from athenaforge.domain.events.event_base import DomainEvent


@dataclass(frozen=True)
class WavePlanned(DomainEvent):
    """Emitted when wave planning completes."""

    wave_count: int = 0
    total_tables: int = 0


@dataclass(frozen=True)
class WaveStarted(DomainEvent):
    """Emitted when a wave begins execution."""

    wave_id: str = ""
    mode: str = ""


@dataclass(frozen=True)
class ParallelRunModeChanged(DomainEvent):
    """Emitted when the parallel-run mode for a wave changes."""

    wave_id: str = ""
    old_mode: str = ""
    new_mode: str = ""


@dataclass(frozen=True)
class RollbackTriggered(DomainEvent):
    """Emitted when a wave rollback is triggered."""

    wave_id: str = ""
    reason: str = ""


@dataclass(frozen=True)
class WaveGatePassed(DomainEvent):
    """Emitted when a wave passes its quality gate."""

    wave_id: str = ""
    criteria_met: tuple[str, ...] = ()


@dataclass(frozen=True)
class WaveGateFailed(DomainEvent):
    """Emitted when a wave fails its quality gate."""

    wave_id: str = ""
    criteria_failed: tuple[str, ...] = ()


@dataclass(frozen=True)
class WaveCompleted(DomainEvent):
    """Emitted when a wave completes migration."""

    wave_id: str = ""
    tables_migrated: int = 0
