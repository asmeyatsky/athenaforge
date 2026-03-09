from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Tier(Enum):
    TIER_1 = "TIER_1"
    TIER_2 = "TIER_2"
    TIER_3 = "TIER_3"


@dataclass(frozen=True)
class TierClassification:
    table_name: str
    tier: Tier
    reason: str
    size_bytes: int
    last_queried_days_ago: int | None
