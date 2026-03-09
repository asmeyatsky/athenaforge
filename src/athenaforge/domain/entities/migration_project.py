from __future__ import annotations

from dataclasses import dataclass, field, replace

from athenaforge.domain.events.event_base import DomainEvent
from athenaforge.domain.events.foundation_events import ScaffoldGenerated


@dataclass(frozen=True)
class MigrationProject:
    """Root aggregate representing an end-to-end migration project."""

    project_id: str
    name: str
    gcp_project_id: str
    aws_region: str
    lobs: tuple[str, ...] = ()
    status: str = "initialized"
    _events: list[DomainEvent] = field(default_factory=list, repr=False)

    # ── commands ────────────────────────────────────────────────

    def add_lob(self, lob_name: str) -> MigrationProject:
        """Return a new project with *lob_name* appended to the LOB list."""
        new_project = replace(self, lobs=(*self.lobs, lob_name))
        new_project._events.append(
            ScaffoldGenerated(
                aggregate_id=self.project_id,
                lob_name=lob_name,
            )
        )
        return new_project

    def start_scaffolding(self) -> MigrationProject:
        """Transition the project into the scaffolding phase."""
        new_project = replace(self, status="scaffolding")
        return new_project

    # ── event harvesting ────────────────────────────────────────

    def collect_events(self) -> list[DomainEvent]:
        """Return accumulated events and clear the internal list."""
        events = list(self._events)
        self._events.clear()
        return events
