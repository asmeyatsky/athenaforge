from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class LOB:
    name: str
    owner: str
    datasets: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class LOBManifest:
    project_id: str
    lobs: tuple[LOB, ...]
    created_at: datetime
