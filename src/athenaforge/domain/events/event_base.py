from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from uuid import uuid4


@dataclass(frozen=True)
class DomainEvent:
    """Base class for all domain events in AthenaForge."""

    aggregate_id: str
    event_id: str = field(default_factory=lambda: uuid4().hex)
    occurred_at: datetime = field(default_factory=datetime.utcnow)
    event_type: str = field(default="")

    def __post_init__(self) -> None:
        if not self.event_type:
            object.__setattr__(self, "event_type", type(self).__name__)
