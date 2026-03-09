from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime

from athenaforge.domain.events.event_base import DomainEvent
from athenaforge.domain.events.transfer_events import (
    TransferJobCompleted,
    TransferJobCreated,
)


@dataclass(frozen=True)
class TransferJob:
    """Aggregate representing a data transfer between cloud storage buckets."""

    job_id: str
    source_bucket: str
    destination_bucket: str
    total_bytes: int
    bytes_transferred: int = 0
    status: str = "pending"
    created_at: datetime = field(default_factory=datetime.utcnow)
    _events: list[DomainEvent] = field(default_factory=list, repr=False)

    # ── queries ─────────────────────────────────────────────────

    @property
    def progress_percentage(self) -> float:
        """Percentage of total bytes that have been transferred."""
        if self.total_bytes == 0:
            return 100.0
        return (self.bytes_transferred / self.total_bytes) * 100.0

    # ── commands ────────────────────────────────────────────────

    def mark_completed(self, bytes_transferred: int) -> TransferJob:
        """Return a new job marked as completed with the final byte count."""
        completed = replace(
            self,
            status="completed",
            bytes_transferred=bytes_transferred,
        )
        completed._events.append(
            TransferJobCompleted(
                aggregate_id=self.job_id,
                job_id=self.job_id,
                bytes_transferred=bytes_transferred,
            )
        )
        return completed

    def mark_failed(self, error: str) -> TransferJob:
        """Return a new job marked as failed."""
        failed = replace(self, status="failed")
        return failed

    # ── event harvesting ────────────────────────────────────────

    def collect_events(self) -> list[DomainEvent]:
        """Return accumulated events and clear the internal list."""
        events = list(self._events)
        self._events.clear()
        return events
