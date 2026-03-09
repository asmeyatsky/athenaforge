from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from athenaforge.domain.entities.table_inventory import TableEntry
from athenaforge.domain.services.tier_classification_service import TierClassificationService
from athenaforge.domain.value_objects.tier import Tier

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW = datetime.now(tz=timezone.utc)


def _make_table(
    *,
    name: str = "db.my_table",
    size_bytes: int = 500 * 1024**3,
    days_ago: int | None = 30,
    database: str = "analytics",
    row_count: int = 1_000_000,
    partitioned: bool = True,
    fmt: str = "PARQUET",
) -> TableEntry:
    last_queried = _NOW - timedelta(days=days_ago) if days_ago is not None else None
    return TableEntry(
        table_name=name,
        database=database,
        size_bytes=size_bytes,
        row_count=row_count,
        last_queried=last_queried,
        partitioned=partitioned,
        format=fmt,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def svc() -> TierClassificationService:
    return TierClassificationService()


# ---------------------------------------------------------------------------
# Individual classification tests
# ---------------------------------------------------------------------------

class TestClassify:
    """Tests for TierClassificationService.classify."""

    def test_active_small_table_is_tier_1(self, svc: TierClassificationService) -> None:
        table = _make_table(size_bytes=500 * 1024**3, days_ago=30)
        result = svc.classify(table)

        assert result.tier == Tier.TIER_1
        assert result.table_name == table.table_name
        assert result.size_bytes == table.size_bytes
        assert result.last_queried_days_ago is not None
        assert result.last_queried_days_ago == 30

    def test_active_large_table_is_tier_2(self, svc: TierClassificationService) -> None:
        two_tb = 2 * 1024**4  # 2 TB
        table = _make_table(size_bytes=two_tb, days_ago=30)
        result = svc.classify(table)

        assert result.tier == Tier.TIER_2
        assert result.size_bytes == two_tb

    def test_inactive_table_is_tier_3(self, svc: TierClassificationService) -> None:
        table = _make_table(days_ago=100)
        result = svc.classify(table)

        assert result.tier == Tier.TIER_3
        assert result.last_queried_days_ago is not None
        assert result.last_queried_days_ago == 100

    def test_never_queried_table_is_tier_3(self, svc: TierClassificationService) -> None:
        table = _make_table(days_ago=None)
        result = svc.classify(table)

        assert result.tier == Tier.TIER_3
        assert result.last_queried_days_ago is None
        assert "never been queried" in result.reason.lower()


class TestBoundaryClassification:
    """Edge-case boundary tests."""

    def test_89_days_small_is_tier_1(self, svc: TierClassificationService) -> None:
        table = _make_table(size_bytes=500 * 1024**3, days_ago=89)
        result = svc.classify(table)
        assert result.tier == Tier.TIER_1

    def test_90_days_is_tier_3(self, svc: TierClassificationService) -> None:
        table = _make_table(days_ago=90)
        result = svc.classify(table)
        assert result.tier == Tier.TIER_3

    def test_size_just_below_one_tb_is_tier_1(self, svc: TierClassificationService) -> None:
        one_tb = 1_099_511_627_776  # 1 TiB in bytes
        table = _make_table(size_bytes=one_tb - 1, days_ago=30)
        result = svc.classify(table)
        assert result.tier == Tier.TIER_1

    def test_size_exactly_one_tb_is_tier_2(self, svc: TierClassificationService) -> None:
        one_tb = 1_099_511_627_776
        table = _make_table(size_bytes=one_tb, days_ago=30)
        result = svc.classify(table)
        assert result.tier == Tier.TIER_2


# ---------------------------------------------------------------------------
# Batch classification
# ---------------------------------------------------------------------------

class TestClassifyBatch:
    def test_classify_batch_with_all_tiers(self, svc: TierClassificationService) -> None:
        tables = [
            _make_table(name="small_active", size_bytes=100 * 1024**3, days_ago=10),
            _make_table(name="large_active", size_bytes=2 * 1024**4, days_ago=10),
            _make_table(name="inactive", days_ago=120),
            _make_table(name="never_queried", days_ago=None),
        ]

        results = svc.classify_batch(tables)

        assert len(results) == 4
        assert results[0].tier == Tier.TIER_1
        assert results[1].tier == Tier.TIER_2
        assert results[2].tier == Tier.TIER_3
        assert results[3].tier == Tier.TIER_3

    def test_classify_batch_empty_list(self, svc: TierClassificationService) -> None:
        assert svc.classify_batch([]) == []
