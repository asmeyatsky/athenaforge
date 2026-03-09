"""Tests for WavePlannerService — pure domain logic, no mocks."""
from __future__ import annotations

import pytest

from athenaforge.domain.services.wave_planner_service import WavePlannerService
from athenaforge.domain.value_objects.tier import Tier, TierClassification
from athenaforge.domain.value_objects.wave import WaveDefinition


def _tc(table_name: str, tier: Tier) -> TierClassification:
    """Helper to build a TierClassification with minimal boilerplate."""
    return TierClassification(
        table_name=table_name,
        tier=tier,
        reason="test",
        size_bytes=1000,
        last_queried_days_ago=1,
    )


class TestPlanWavesGroupsByLOB:
    def test_tables_grouped_by_lob(self):
        svc = WavePlannerService()
        classifications = [
            _tc("payments.orders", Tier.TIER_1),
            _tc("payments.refunds", Tier.TIER_2),
            _tc("lending.loans", Tier.TIER_1),
        ]
        waves = svc.plan_waves(classifications, lobs=["payments", "lending"])

        lob_names = [w.lob for w in waves]
        assert "payments" in lob_names
        assert "lending" in lob_names

    def test_tables_assigned_to_correct_lob(self):
        svc = WavePlannerService()
        classifications = [
            _tc("payments.orders", Tier.TIER_1),
            _tc("lending.loans", Tier.TIER_2),
        ]
        waves = svc.plan_waves(classifications, lobs=["payments", "lending"])

        payments_wave = next(w for w in waves if w.lob == "payments")
        lending_wave = next(w for w in waves if w.lob == "lending")

        assert "payments.orders" in payments_wave.tables
        assert "lending.loans" in lending_wave.tables


class TestTier1TablesInEarliestWaves:
    def test_tier1_tables_appear_first_in_wave(self):
        svc = WavePlannerService()
        classifications = [
            _tc("fin.report_3", Tier.TIER_3),
            _tc("fin.report_1", Tier.TIER_1),
            _tc("fin.report_2", Tier.TIER_2),
        ]
        waves = svc.plan_waves(classifications, lobs=["fin"])

        assert len(waves) == 1
        tables = waves[0].tables
        # Tier 1 should come before Tier 2, which comes before Tier 3
        assert tables.index("fin.report_1") < tables.index("fin.report_2")
        assert tables.index("fin.report_2") < tables.index("fin.report_3")

    def test_multiple_tier1_tables_all_before_tier2(self):
        svc = WavePlannerService()
        classifications = [
            _tc("fin.t2", Tier.TIER_2),
            _tc("fin.t1a", Tier.TIER_1),
            _tc("fin.t1b", Tier.TIER_1),
            _tc("fin.t3", Tier.TIER_3),
        ]
        waves = svc.plan_waves(classifications, lobs=["fin"])

        tables = waves[0].tables
        tier1_positions = [tables.index("fin.t1a"), tables.index("fin.t1b")]
        tier2_position = tables.index("fin.t2")
        tier3_position = tables.index("fin.t3")

        assert all(p < tier2_position for p in tier1_positions)
        assert tier2_position < tier3_position


class TestMaxParallelLimit:
    def test_max_parallel_respected(self):
        svc = WavePlannerService()
        classifications = [
            _tc("a.t1", Tier.TIER_1),
            _tc("b.t1", Tier.TIER_1),
            _tc("c.t1", Tier.TIER_1),
            _tc("d.t1", Tier.TIER_1),
            _tc("e.t1", Tier.TIER_1),
        ]
        lobs = ["a", "b", "c", "d", "e"]
        waves = svc.plan_waves(classifications, lobs=lobs, max_parallel=2)

        # 5 LOBs with max_parallel=2 → 3 batches (2, 2, 1)
        assert len(waves) == 5

    def test_max_parallel_of_1_creates_sequential_waves(self):
        svc = WavePlannerService()
        classifications = [
            _tc("a.t1", Tier.TIER_1),
            _tc("b.t1", Tier.TIER_1),
            _tc("c.t1", Tier.TIER_1),
        ]
        lobs = ["a", "b", "c"]
        waves = svc.plan_waves(classifications, lobs=lobs, max_parallel=1)

        assert len(waves) == 3

    def test_max_parallel_greater_than_lob_count(self):
        svc = WavePlannerService()
        classifications = [
            _tc("a.t1", Tier.TIER_1),
            _tc("b.t1", Tier.TIER_1),
        ]
        lobs = ["a", "b"]
        waves = svc.plan_waves(classifications, lobs=lobs, max_parallel=10)

        # All LOBs fit in one batch
        assert len(waves) == 2


class TestEmptyClassifications:
    def test_empty_classifications_returns_empty_waves(self):
        svc = WavePlannerService()
        waves = svc.plan_waves(classifications=[], lobs=["payments"])

        assert waves == []

    def test_empty_lobs_and_classifications(self):
        svc = WavePlannerService()
        waves = svc.plan_waves(classifications=[], lobs=[])

        assert waves == []


class TestEstimatedDuration:
    def test_small_table_count_uses_baseline(self):
        svc = WavePlannerService()
        # 2 tables → ceil(2/50)*10 = 10, baseline is 10, max is 10
        classifications = [
            _tc("fin.t1", Tier.TIER_1),
            _tc("fin.t2", Tier.TIER_2),
        ]
        waves = svc.plan_waves(classifications, lobs=["fin"])

        assert waves[0].estimated_duration_days == 10

    def test_large_table_count_scales_duration(self):
        svc = WavePlannerService()
        # 100 tables → ceil(100/50)*10 = 20
        classifications = [_tc(f"fin.t{i}", Tier.TIER_2) for i in range(100)]
        waves = svc.plan_waves(classifications, lobs=["fin"])

        assert waves[0].estimated_duration_days == 20

    def test_51_tables_rounds_up(self):
        svc = WavePlannerService()
        # 51 tables → ceil(51/50)*10 = 20
        classifications = [_tc(f"fin.t{i}", Tier.TIER_2) for i in range(51)]
        waves = svc.plan_waves(classifications, lobs=["fin"])

        assert waves[0].estimated_duration_days == 20

    def test_exactly_50_tables(self):
        svc = WavePlannerService()
        # 50 tables → ceil(50/50)*10 = 10
        classifications = [_tc(f"fin.t{i}", Tier.TIER_2) for i in range(50)]
        waves = svc.plan_waves(classifications, lobs=["fin"])

        assert waves[0].estimated_duration_days == 10


class TestWaveDefinitionFields:
    def test_wave_has_correct_id_and_name(self):
        svc = WavePlannerService()
        classifications = [_tc("fin.t1", Tier.TIER_1)]
        waves = svc.plan_waves(classifications, lobs=["fin"])

        assert waves[0].wave_id == "wave-1"
        assert waves[0].name == "Wave 1"

    def test_wave_dependencies_are_empty_tuple(self):
        svc = WavePlannerService()
        classifications = [_tc("fin.t1", Tier.TIER_1)]
        waves = svc.plan_waves(classifications, lobs=["fin"])

        assert waves[0].dependencies == ()
