from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class WaveStatus(Enum):
    PLANNED = "PLANNED"
    SHADOW_RUNNING = "SHADOW_RUNNING"
    REVERSE_SHADOW_RUNNING = "REVERSE_SHADOW_RUNNING"
    CUTOVER_READY = "CUTOVER_READY"
    CUTTING_OVER = "CUTTING_OVER"
    COMPLETED = "COMPLETED"
    ROLLED_BACK = "ROLLED_BACK"


class ParallelRunMode(Enum):
    OLD_ONLY = "OLD_ONLY"
    SHADOW = "SHADOW"
    REVERSE_SHADOW = "REVERSE_SHADOW"
    NEW_ONLY = "NEW_ONLY"


@dataclass(frozen=True)
class WaveDefinition:
    wave_id: str
    name: str
    lob: str
    tables: tuple[str, ...]
    estimated_duration_days: int
    dependencies: tuple[str, ...]
