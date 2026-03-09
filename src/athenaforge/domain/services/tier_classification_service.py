from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone

from athenaforge.domain.entities.table_inventory import TableEntry
from athenaforge.domain.value_objects.tier import Tier, TierClassification

_ONE_TB = 1_099_511_627_776
_NINETY_DAYS = 90


class TierClassificationService:
    """Pure domain service that assigns a migration tier to each table."""

    def classify(self, table: TableEntry) -> TierClassification:
        now = datetime.now(tz=timezone.utc)

        if table.last_queried is None:
            return TierClassification(
                table_name=table.table_name,
                tier=Tier.TIER_3,
                reason="Table has never been queried",
                size_bytes=table.size_bytes,
                last_queried_days_ago=None,
            )

        last_queried_aware = (
            table.last_queried
            if table.last_queried.tzinfo is not None
            else table.last_queried.replace(tzinfo=timezone.utc)
        )
        days_ago = (now - last_queried_aware).days

        if days_ago >= _NINETY_DAYS:
            return TierClassification(
                table_name=table.table_name,
                tier=Tier.TIER_3,
                reason=f"Not queried in {days_ago} days (>= {_NINETY_DAYS})",
                size_bytes=table.size_bytes,
                last_queried_days_ago=days_ago,
            )

        if table.size_bytes < _ONE_TB:
            return TierClassification(
                table_name=table.table_name,
                tier=Tier.TIER_1,
                reason=f"Queried {days_ago} days ago, size < 1 TB",
                size_bytes=table.size_bytes,
                last_queried_days_ago=days_ago,
            )

        return TierClassification(
            table_name=table.table_name,
            tier=Tier.TIER_2,
            reason=f"Queried {days_ago} days ago, size >= 1 TB",
            size_bytes=table.size_bytes,
            last_queried_days_ago=days_ago,
        )

    def classify_batch(
        self, tables: Sequence[TableEntry]
    ) -> list[TierClassification]:
        return [self.classify(table) for table in tables]
