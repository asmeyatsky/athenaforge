from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Self

from athenaforge.domain.events.event_base import DomainEvent
from athenaforge.domain.events.foundation_events import ScaffoldGenerated
from athenaforge.domain.value_objects.status import ProjectStatus


@dataclass(frozen=True)
class MigrationProject:
    """Root aggregate representing an end-to-end migration project."""

    project_id: str
    name: str
    gcp_project_id: str
    aws_region: str
    lobs: tuple[str, ...] = ()
    status: ProjectStatus = ProjectStatus.INITIALIZED
    _events: tuple[DomainEvent, ...] = field(default=(), repr=False)

    # ── commands ────────────────────────────────────────────────

    def add_lob(self, lob_name: str) -> MigrationProject:
        """Return a new project with *lob_name* appended to the LOB list."""
        event = ScaffoldGenerated(
            aggregate_id=self.project_id,
            lob_name=lob_name,
        )
        return replace(
            self,
            lobs=(*self.lobs, lob_name),
            _events=self._events + (event,),
        )

    def start_scaffolding(self) -> MigrationProject:
        """Transition the project into the scaffolding phase."""
        return replace(self, status=ProjectStatus.SCAFFOLDING)

    # ── event harvesting ────────────────────────────────────────

    def collect_events(self) -> tuple[DomainEvent, ...]:
        """Return accumulated events."""
        return self._events

    def clear_events(self) -> Self:
        """Return a new instance with an empty events tuple."""
        return replace(self, _events=())
