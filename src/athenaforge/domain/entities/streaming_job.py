from __future__ import annotations

from dataclasses import dataclass, field, replace

from athenaforge.domain.events.event_base import DomainEvent
from athenaforge.domain.events.transfer_events import StreamingCutoverInitiated


@dataclass(frozen=True)
class StreamingJob:
    """Aggregate representing a streaming pipeline undergoing migration."""

    job_id: str
    source_topic: str
    target_topic: str
    consumer_group: str
    status: str = "active"
    lag_threshold: int = 1000
    current_lag: int = 0
    _events: list[DomainEvent] = field(default_factory=list, repr=False)

    # ── commands ────────────────────────────────────────────────

    def initiate_drain(self) -> StreamingJob:
        """Begin draining the source topic before cutover."""
        drained = replace(self, status="draining")
        return drained

    def switch_target(self) -> StreamingJob:
        """Switch consumers to the new target topic."""
        switched = replace(self, status="switching")
        switched._events.append(
            StreamingCutoverInitiated(
                aggregate_id=self.job_id,
                job_id=self.job_id,
                source_topic=self.source_topic,
                target_topic=self.target_topic,
            )
        )
        return switched

    def verify_cutover(self) -> StreamingJob:
        """Verify the cutover completed successfully."""
        if self.current_lag > self.lag_threshold:
            raise ValueError(
                f"Current lag ({self.current_lag}) exceeds threshold ({self.lag_threshold})"
            )
        verified = replace(self, status="verified")
        return verified

    # ── event harvesting ────────────────────────────────────────

    def collect_events(self) -> list[DomainEvent]:
        """Return accumulated events and clear the internal list."""
        events = list(self._events)
        self._events.clear()
        return events
