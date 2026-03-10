from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Self

from athenaforge.domain.events.event_base import DomainEvent
from athenaforge.domain.events.transfer_events import StreamingCutoverInitiated
from athenaforge.domain.value_objects.status import StreamingJobStatus


@dataclass(frozen=True)
class StreamingJob:
    """Aggregate representing a streaming pipeline undergoing migration."""

    job_id: str
    source_topic: str
    target_topic: str
    consumer_group: str
    status: StreamingJobStatus = StreamingJobStatus.ACTIVE
    lag_threshold: int = 1000
    current_lag: int = 0
    _events: tuple[DomainEvent, ...] = field(default=(), repr=False)

    # ── commands ────────────────────────────────────────────────

    def initiate_drain(self) -> StreamingJob:
        """Begin draining the source topic before cutover."""
        return replace(self, status=StreamingJobStatus.DRAINING)

    def switch_target(self) -> StreamingJob:
        """Switch consumers to the new target topic."""
        event = StreamingCutoverInitiated(
            aggregate_id=self.job_id,
            job_id=self.job_id,
            source_topic=self.source_topic,
            target_topic=self.target_topic,
        )
        return replace(
            self,
            status=StreamingJobStatus.SWITCHING,
            _events=self._events + (event,),
        )

    def verify_cutover(self) -> StreamingJob:
        """Verify the cutover completed successfully."""
        if self.current_lag > self.lag_threshold:
            raise ValueError(
                f"Current lag ({self.current_lag}) exceeds threshold ({self.lag_threshold})"
            )
        return replace(self, status=StreamingJobStatus.VERIFIED)

    # ── event harvesting ────────────────────────────────────────

    def collect_events(self) -> tuple[DomainEvent, ...]:
        """Return accumulated events."""
        return self._events

    def clear_events(self) -> Self:
        """Return a new instance with an empty events tuple."""
        return replace(self, _events=())
