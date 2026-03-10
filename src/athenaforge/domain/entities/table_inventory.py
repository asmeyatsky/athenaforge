from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import TYPE_CHECKING, Self

from athenaforge.domain.events.event_base import DomainEvent
from athenaforge.domain.events.foundation_events import TierClassificationCompleted
from athenaforge.domain.value_objects.tier import Tier, TierClassification

if TYPE_CHECKING:
    from athenaforge.domain.services.tier_classification_service import (
        TierClassificationService,
    )


@dataclass(frozen=True)
class TableEntry:
    """Immutable record describing a single source table."""

    table_name: str
    database: str
    size_bytes: int
    row_count: int
    last_queried: datetime | None
    partitioned: bool
    format: str = "PARQUET"
    has_maps: bool = False


@dataclass(frozen=True)
class TableInventory:
    """Aggregate that holds the full table inventory and tier classifications."""

    inventory_id: str
    tables: tuple[TableEntry, ...] = ()
    classifications: dict[str, TierClassification] = field(default_factory=dict)
    _events: tuple[DomainEvent, ...] = field(default=(), repr=False)

    # ── commands ────────────────────────────────────────────────

    def classify_all(self, service: TierClassificationService) -> TableInventory:
        """Classify every table using the supplied classification service."""
        new_classifications: dict[str, TierClassification] = {}
        tier_counts: dict[Tier, int] = {Tier.TIER_1: 0, Tier.TIER_2: 0, Tier.TIER_3: 0}

        for table in self.tables:
            classification: TierClassification = service.classify(table)
            new_classifications[table.table_name] = classification
            tier_counts[classification.tier] = tier_counts.get(classification.tier, 0) + 1

        event = TierClassificationCompleted(
            aggregate_id=self.inventory_id,
            total_tables=len(self.tables),
            tier1_count=tier_counts[Tier.TIER_1],
            tier2_count=tier_counts[Tier.TIER_2],
            tier3_count=tier_counts[Tier.TIER_3],
        )
        return replace(
            self,
            classifications=new_classifications,
            _events=self._events + (event,),
        )

    # ── queries ─────────────────────────────────────────────────

    def get_by_tier(self, tier: Tier) -> tuple[TableEntry, ...]:
        """Return all tables classified under *tier*."""
        return tuple(
            table
            for table in self.tables
            if table.table_name in self.classifications
            and self.classifications[table.table_name].tier == tier
        )

    def get_tables_with_maps(self) -> tuple[TableEntry, ...]:
        """Return all tables that use MAP columns."""
        return tuple(table for table in self.tables if table.has_maps)

    # ── event harvesting ────────────────────────────────────────

    def collect_events(self) -> tuple[DomainEvent, ...]:
        """Return accumulated events."""
        return self._events

    def clear_events(self) -> Self:
        """Return a new instance with an empty events tuple."""
        return replace(self, _events=())
