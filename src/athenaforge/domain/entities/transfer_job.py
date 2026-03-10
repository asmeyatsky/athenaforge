from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Self

from athenaforge.domain.events.event_base import DomainEvent
from athenaforge.domain.events.transfer_events import TransferJobCompleted
from athenaforge.domain.value_objects.status import TransferStatus


@dataclass(frozen=True)
class TransferJob:
    """Aggregate representing a data transfer between cloud storage buckets."""

    job_id: str
    source_bucket: str
    destination_bucket: str
    total_bytes: int
    bytes_transferred: int = 0
    status: TransferStatus = TransferStatus.PENDING
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    _events: tuple[DomainEvent, ...] = field(default=(), repr=False)

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
        event = TransferJobCompleted(
            aggregate_id=self.job_id,
            job_id=self.job_id,
            bytes_transferred=bytes_transferred,
        )
        return replace(
            self,
            status=TransferStatus.COMPLETED,
            bytes_transferred=bytes_transferred,
            _events=self._events + (event,),
        )

    def mark_failed(self, error: str) -> TransferJob:
        """Return a new job marked as failed."""
        return replace(self, status=TransferStatus.FAILED)

    # ── event harvesting ────────────────────────────────────────

    def collect_events(self) -> tuple[DomainEvent, ...]:
        """Return accumulated events."""
        return self._events

    def clear_events(self) -> Self:
        """Return a new instance with an empty events tuple."""
        return replace(self, _events=())
