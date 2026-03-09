from __future__ import annotations

import math
from collections import defaultdict

from athenaforge.domain.value_objects.tier import Tier, TierClassification
from athenaforge.domain.value_objects.wave import WaveDefinition

_BASELINE_TABLES = 50
_BASELINE_DAYS = 10


class WavePlannerService:
    """Pure domain service that groups classified tables into migration waves."""

    def plan_waves(
        self,
        classifications: list[TierClassification],
        lobs: list[str],
        max_parallel: int = 3,
    ) -> list[WaveDefinition]:
        """Plan migration waves.

        Tables are grouped by LOB, Tier-1 tables are scheduled in the
        earliest waves, and ``max_parallel`` controls how many LOBs may
        run concurrently within the same wave.
        """
        # Bucket classifications by LOB
        lob_set = set(lobs)
        tables_by_lob: dict[str, list[TierClassification]] = defaultdict(list)
        for c in classifications:
            # Determine LOB from the table name prefix (first segment before '.')
            assigned_lob = self._resolve_lob(c.table_name, lob_set)
            tables_by_lob[assigned_lob].append(c)

        # Sort each LOB's tables: Tier 1 first, then Tier 2, then Tier 3
        tier_order = {Tier.TIER_1: 0, Tier.TIER_2: 1, Tier.TIER_3: 2}
        for lob_tables in tables_by_lob.values():
            lob_tables.sort(key=lambda tc: tier_order.get(tc.tier, 99))

        # Build waves respecting max_parallel
        waves: list[WaveDefinition] = []
        wave_number = 0
        lob_keys = sorted(tables_by_lob.keys())

        for batch_start in range(0, len(lob_keys), max_parallel):
            batch_lobs = lob_keys[batch_start : batch_start + max_parallel]
            # For each LOB in the batch, create a wave
            for lob in batch_lobs:
                wave_number += 1
                lob_tables = tables_by_lob[lob]
                table_names = tuple(tc.table_name for tc in lob_tables)
                table_count = len(table_names)
                duration = max(
                    _BASELINE_DAYS,
                    math.ceil(table_count / _BASELINE_TABLES) * _BASELINE_DAYS,
                )
                waves.append(
                    WaveDefinition(
                        wave_id=f"wave-{wave_number}",
                        name=f"Wave {wave_number}",
                        lob=lob,
                        tables=table_names,
                        estimated_duration_days=duration,
                        dependencies=(),
                    )
                )

        return waves

    @staticmethod
    def _resolve_lob(table_name: str, lob_set: set[str]) -> str:
        """Best-effort LOB resolution from the table name or the first LOB."""
        # Try prefix match (e.g. "payments.orders" → "payments")
        for lob in lob_set:
            if table_name.lower().startswith(lob.lower()):
                return lob
        # Fallback: assign to first LOB alphabetically
        return sorted(lob_set)[0] if lob_set else "default"
