from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class LOB:
    name: str
    owner: str
    datasets: tuple[str, ...] = ()


@dataclass(frozen=True)
class LOBManifest:
    project_id: str
    lobs: tuple[LOB, ...]
    created_at: datetime
