"""Application-layer tests for Wave module use cases."""

from __future__ import annotations

import pytest

from athenaforge.application.commands.wave.plan_waves import PlanWavesUseCase
from athenaforge.application.commands.wave.control_parallel_run import (
    ControlParallelRunUseCase,
)
from athenaforge.application.commands.wave.evaluate_rollback import (
    EvaluateRollbackUseCase,
)
from athenaforge.application.commands.wave.enforce_wave_gate import (
    EnforceWaveGateUseCase,
)
from athenaforge.application.commands.wave.migrate_dashboards import (
    MigrateDashboardsUseCase,
)
from athenaforge.application.commands.wave.reconcile_kpis import (
    ReconcileKPIsUseCase,
)
from athenaforge.domain.entities.table_inventory import TableInventory
from athenaforge.domain.entities.wave import Wave
from athenaforge.domain.events.event_base import DomainEvent
from athenaforge.domain.events.wave_events import (
    ParallelRunModeChanged,
    RollbackTriggered,
    WaveGateFailed,
    WaveGatePassed,
    WavePlanned,
)
from athenaforge.domain.services.parallel_running_state_machine import (
    ParallelRunningStateMachine,
)
from athenaforge.domain.services.rollback_evaluator import RollbackEvaluator
from athenaforge.domain.services.wave_planner_service import WavePlannerService
from athenaforge.domain.value_objects.tier import Tier, TierClassification
from athenaforge.domain.value_objects.wave import ParallelRunMode, WaveStatus


# ── Stub ports ───────────────────────────────────────────────────────────────


class StubEventBus:
    """Collects all published events."""

    def __init__(self) -> None:
        self.events: list[DomainEvent] = []

    async def publish(self, event: DomainEvent) -> None:
        self.events.append(event)

    def subscribe(self, event_type: type, handler: object) -> None:
        pass


class StubTableInventoryRepo:
    """In-memory read repository for TableInventory entities."""

    def __init__(self, inventories: list[TableInventory] | None = None) -> None:
        self._store: dict[str, TableInventory] = {
            inv.inventory_id: inv for inv in (inventories or [])
        }

    async def get_by_id(self, id: str) -> TableInventory | None:
        return self._store.get(id)

    async def list_all(self) -> list[TableInventory]:
        return list(self._store.values())


class StubWaveRepositoryPort:
    """In-memory combined read/write repository for Wave entities."""

    def __init__(self, waves: list[Wave] | None = None) -> None:
        self._store: dict[str, Wave] = {
            w.wave_id: w for w in (waves or [])
        }

    async def get_by_id(self, id: str) -> Wave | None:
        return self._store.get(id)

    async def save(self, entity: Wave) -> None:
        self._store[entity.wave_id] = entity

    async def delete(self, id: str) -> None:
        self._store.pop(id, None)

    async def list_all(self) -> list[Wave]:
        return list(self._store.values())


# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_inventory(
    inventory_id: str, classifications: dict[str, TierClassification]
) -> TableInventory:
    """Build a TableInventory with pre-set classifications (no tables needed)."""
    return TableInventory(
        inventory_id=inventory_id,
        tables=(),
        classifications=classifications,
    )


def _make_classification(table_name: str, tier: Tier) -> TierClassification:
    return TierClassification(
        table_name=table_name,
        tier=tier,
        reason="test",
        size_bytes=1_000_000,
        last_queried_days_ago=5,
    )


# ── PlanWavesUseCase ─────────────────────────────────────────────────────────


async def test_plan_waves_creates_waves_grouped_by_lob():
    """Each LOB should produce its own wave."""
    classifications = {
        "payments.orders": _make_classification("payments.orders", Tier.TIER_1),
        "payments.refunds": _make_classification("payments.refunds", Tier.TIER_2),
        "marketing.clicks": _make_classification("marketing.clicks", Tier.TIER_1),
    }
    inventory = _make_inventory("inv-1", classifications)
    repo = StubTableInventoryRepo([inventory])
    planner = WavePlannerService()
    bus = StubEventBus()

    uc = PlanWavesUseCase(planner, repo, bus)
    result = await uc.execute("inv-1", ["payments", "marketing"], max_parallel=5)

    # Should have at least 2 waves (one per LOB)
    assert result.total_waves >= 2
    lobs_in_waves = {w["lob"] for w in result.waves}
    assert "payments" in lobs_in_waves
    assert "marketing" in lobs_in_waves


async def test_plan_waves_respects_max_parallel_limit():
    """With max_parallel=1, LOBs should be batched sequentially."""
    classifications = {
        "alpha.t1": _make_classification("alpha.t1", Tier.TIER_1),
        "beta.t1": _make_classification("beta.t1", Tier.TIER_1),
        "gamma.t1": _make_classification("gamma.t1", Tier.TIER_1),
    }
    inventory = _make_inventory("inv-2", classifications)
    repo = StubTableInventoryRepo([inventory])
    planner = WavePlannerService()
    bus = StubEventBus()

    uc = PlanWavesUseCase(planner, repo, bus)
    result = await uc.execute(
        "inv-2", ["alpha", "beta", "gamma"], max_parallel=1
    )

    # With max_parallel=1, we still get 3 waves (one per LOB), but batched
    assert result.total_waves == 3


async def test_plan_waves_publishes_wave_planned_event():
    """A WavePlanned event should be published once."""
    classifications = {
        "lob_a.t1": _make_classification("lob_a.t1", Tier.TIER_1),
    }
    inventory = _make_inventory("inv-3", classifications)
    repo = StubTableInventoryRepo([inventory])
    planner = WavePlannerService()
    bus = StubEventBus()

    uc = PlanWavesUseCase(planner, repo, bus)
    await uc.execute("inv-3", ["lob_a"])

    planned_events = [e for e in bus.events if isinstance(e, WavePlanned)]
    assert len(planned_events) == 1
    assert planned_events[0].total_tables >= 1


async def test_plan_waves_raises_when_inventory_not_found():
    """Should raise ValueError when the inventory ID does not exist."""
    repo = StubTableInventoryRepo([])
    planner = WavePlannerService()
    bus = StubEventBus()

    uc = PlanWavesUseCase(planner, repo, bus)
    with pytest.raises(ValueError, match="not found"):
        await uc.execute("non-existent", ["payments"])


# ── ControlParallelRunUseCase ────────────────────────────────────────────────


async def test_control_parallel_run_transitions_old_only_to_shadow():
    """Should transition from OLD_ONLY to SHADOW."""
    wave = Wave(
        wave_id="w-1",
        name="Wave 1",
        lob="payments",
        tables=("t1", "t2"),
        status=WaveStatus.PLANNED,
        mode=ParallelRunMode.OLD_ONLY,
    )
    wave_repo = StubWaveRepositoryPort([wave])
    sm = ParallelRunningStateMachine()
    bus = StubEventBus()

    uc = ControlParallelRunUseCase(sm, wave_repo, bus)
    result = await uc.execute("w-1", "SHADOW")

    assert result.wave_id == "w-1"
    assert result.previous_mode == "OLD_ONLY"
    assert result.current_mode == "SHADOW"
    assert result.success is True


async def test_control_parallel_run_publishes_mode_changed_event():
    """A ParallelRunModeChanged event should be published on transition."""
    wave = Wave(
        wave_id="w-2",
        name="Wave 2",
        lob="marketing",
        tables=("t1",),
        mode=ParallelRunMode.OLD_ONLY,
    )
    wave_repo = StubWaveRepositoryPort([wave])
    sm = ParallelRunningStateMachine()
    bus = StubEventBus()

    uc = ControlParallelRunUseCase(sm, wave_repo, bus)
    await uc.execute("w-2", "SHADOW")

    mode_events = [
        e for e in bus.events if isinstance(e, ParallelRunModeChanged)
    ]
    assert len(mode_events) == 1
    assert mode_events[0].old_mode == "OLD_ONLY"
    assert mode_events[0].new_mode == "SHADOW"
    assert mode_events[0].wave_id == "w-2"


async def test_control_parallel_run_persists_updated_wave():
    """The wave repository should hold the updated wave after transition."""
    wave = Wave(
        wave_id="w-3",
        name="Wave 3",
        lob="lob",
        tables=("t1",),
        mode=ParallelRunMode.OLD_ONLY,
    )
    wave_repo = StubWaveRepositoryPort([wave])
    sm = ParallelRunningStateMachine()
    bus = StubEventBus()

    uc = ControlParallelRunUseCase(sm, wave_repo, bus)
    await uc.execute("w-3", "SHADOW")

    persisted = await wave_repo.get_by_id("w-3")
    assert persisted is not None
    assert persisted.mode == ParallelRunMode.SHADOW


async def test_control_parallel_run_handles_invalid_transition():
    """An invalid transition should raise ValueError."""
    wave = Wave(
        wave_id="w-4",
        name="Wave 4",
        lob="lob",
        tables=("t1",),
        mode=ParallelRunMode.OLD_ONLY,
    )
    wave_repo = StubWaveRepositoryPort([wave])
    sm = ParallelRunningStateMachine()
    bus = StubEventBus()

    uc = ControlParallelRunUseCase(sm, wave_repo, bus)
    # Cannot jump from OLD_ONLY directly to NEW_ONLY
    with pytest.raises(ValueError, match="Invalid transition"):
        await uc.execute("w-4", "NEW_ONLY")


# ── EvaluateRollbackUseCase ──────────────────────────────────────────────────


async def test_evaluate_rollback_no_rollback_when_healthy():
    """No rollback should be triggered when all metrics are healthy."""
    evaluator = RollbackEvaluator()
    bus = StubEventBus()

    uc = EvaluateRollbackUseCase(evaluator, bus)
    result = await uc.execute(
        wave_id="w-1",
        dvt_pass_rate=1.0,
        latency_increase_pct=5.0,
        data_loss_detected=False,
        streaming_lag=100,
        escalation_raised=False,
    )

    assert result.should_rollback is False
    assert all(not c["triggered"] for c in result.conditions)


async def test_evaluate_rollback_triggers_on_data_loss():
    """Data loss should trigger rollback."""
    evaluator = RollbackEvaluator()
    bus = StubEventBus()

    uc = EvaluateRollbackUseCase(evaluator, bus)
    result = await uc.execute(
        wave_id="w-2",
        dvt_pass_rate=1.0,
        latency_increase_pct=0.0,
        data_loss_detected=True,
        streaming_lag=0,
        escalation_raised=False,
    )

    assert result.should_rollback is True
    data_loss_conditions = [
        c for c in result.conditions if c["name"] == "Data loss"
    ]
    assert len(data_loss_conditions) == 1
    assert data_loss_conditions[0]["triggered"] is True


async def test_evaluate_rollback_triggers_on_low_dvt_pass_rate():
    """DVT pass rate below threshold should trigger rollback."""
    evaluator = RollbackEvaluator()
    bus = StubEventBus()

    uc = EvaluateRollbackUseCase(evaluator, bus)
    result = await uc.execute(
        wave_id="w-3",
        dvt_pass_rate=0.95,
        latency_increase_pct=0.0,
        data_loss_detected=False,
        streaming_lag=0,
        escalation_raised=False,
    )

    assert result.should_rollback is True
    dvt_conditions = [
        c for c in result.conditions if c["name"] == "DVT pass rate"
    ]
    assert len(dvt_conditions) == 1
    assert dvt_conditions[0]["triggered"] is True


async def test_evaluate_rollback_publishes_event_when_triggered():
    """A RollbackTriggered event should be published when rollback is warranted."""
    evaluator = RollbackEvaluator()
    bus = StubEventBus()

    uc = EvaluateRollbackUseCase(evaluator, bus)
    await uc.execute(
        wave_id="w-4",
        dvt_pass_rate=0.50,
        latency_increase_pct=0.0,
        data_loss_detected=True,
        streaming_lag=0,
        escalation_raised=False,
    )

    rollback_events = [
        e for e in bus.events if isinstance(e, RollbackTriggered)
    ]
    assert len(rollback_events) == 1
    assert rollback_events[0].wave_id == "w-4"
    assert "Data loss" in rollback_events[0].reason


async def test_evaluate_rollback_does_not_publish_event_when_healthy():
    """No RollbackTriggered event should be published when everything is healthy."""
    evaluator = RollbackEvaluator()
    bus = StubEventBus()

    uc = EvaluateRollbackUseCase(evaluator, bus)
    await uc.execute(
        wave_id="w-5",
        dvt_pass_rate=1.0,
        latency_increase_pct=5.0,
        data_loss_detected=False,
        streaming_lag=100,
        escalation_raised=False,
    )

    rollback_events = [
        e for e in bus.events if isinstance(e, RollbackTriggered)
    ]
    assert len(rollback_events) == 0


# ── EnforceWaveGateUseCase ───────────────────────────────────────────────────


async def test_enforce_wave_gate_passes_when_all_criteria_met():
    """Gate should pass when every required criterion is True."""
    bus = StubEventBus()

    uc = EnforceWaveGateUseCase(bus)
    result = await uc.execute(
        "w-1",
        {
            "dvt_passed": True,
            "latency_ok": True,
            "no_data_loss": True,
            "streaming_stable": True,
            "dashboards_verified": True,
            "kpis_reconciled": True,
        },
    )

    assert result.passed is True
    assert len(result.criteria_met) == 6
    assert result.criteria_failed == []


async def test_enforce_wave_gate_fails_when_any_criterion_not_met():
    """Gate should fail when at least one criterion is False."""
    bus = StubEventBus()

    uc = EnforceWaveGateUseCase(bus)
    result = await uc.execute(
        "w-2",
        {
            "dvt_passed": True,
            "latency_ok": True,
            "no_data_loss": False,  # fails
            "streaming_stable": True,
            "dashboards_verified": True,
            "kpis_reconciled": True,
        },
    )

    assert result.passed is False
    assert "no_data_loss" in result.criteria_failed


async def test_enforce_wave_gate_publishes_passed_event():
    """A WaveGatePassed event should be published when the gate passes."""
    bus = StubEventBus()

    uc = EnforceWaveGateUseCase(bus)
    await uc.execute(
        "w-3",
        {
            "dvt_passed": True,
            "latency_ok": True,
            "no_data_loss": True,
            "streaming_stable": True,
            "dashboards_verified": True,
            "kpis_reconciled": True,
        },
    )

    passed_events = [e for e in bus.events if isinstance(e, WaveGatePassed)]
    assert len(passed_events) == 1
    assert passed_events[0].wave_id == "w-3"
    assert len(passed_events[0].criteria_met) == 6


async def test_enforce_wave_gate_publishes_failed_event():
    """A WaveGateFailed event should be published when the gate fails."""
    bus = StubEventBus()

    uc = EnforceWaveGateUseCase(bus)
    await uc.execute(
        "w-4",
        {
            "dvt_passed": False,
            "latency_ok": True,
            "no_data_loss": True,
            "streaming_stable": True,
            "dashboards_verified": True,
            "kpis_reconciled": True,
        },
    )

    failed_events = [e for e in bus.events if isinstance(e, WaveGateFailed)]
    assert len(failed_events) == 1
    assert failed_events[0].wave_id == "w-4"
    assert "dvt_passed" in failed_events[0].criteria_failed


async def test_enforce_wave_gate_identifies_specific_failed_criteria():
    """All failing criteria should be listed in criteria_failed."""
    bus = StubEventBus()

    uc = EnforceWaveGateUseCase(bus)
    result = await uc.execute(
        "w-5",
        {
            "dvt_passed": False,
            "latency_ok": False,
            "no_data_loss": True,
            "streaming_stable": True,
            "dashboards_verified": False,
            "kpis_reconciled": True,
        },
    )

    assert result.passed is False
    assert "dvt_passed" in result.criteria_failed
    assert "latency_ok" in result.criteria_failed
    assert "dashboards_verified" in result.criteria_failed
    assert len(result.criteria_failed) == 3


# ── MigrateDashboardsUseCase ────────────────────────────────────────────────


async def test_migrate_dashboards_migrates_all():
    """All provided dashboard configs should be processed and migrated."""
    uc = MigrateDashboardsUseCase()
    result = await uc.execute([
        {"name": "dash-1", "source": "quicksight"},
        {"name": "dash-2", "source": "quicksight"},
        {"name": "dash-3", "source": "quicksight"},
    ])

    assert result.dashboards_migrated == 3
    assert result.dashboards_failed == 0


async def test_migrate_dashboards_returns_correct_count():
    """Migration count should match the number of dashboard configs."""
    uc = MigrateDashboardsUseCase()
    result = await uc.execute([{"name": "single-dash"}])

    assert result.dashboards_migrated == 1
    assert len(result.details) == 1
    assert result.details[0]["name"] == "single-dash"
    assert result.details[0]["status"] == "migrated"


async def test_migrate_dashboards_handles_empty_config():
    """An empty config list should produce zero migrations."""
    uc = MigrateDashboardsUseCase()
    result = await uc.execute([])

    assert result.dashboards_migrated == 0
    assert result.dashboards_failed == 0
    assert result.details == []


# ── ReconcileKPIsUseCase ─────────────────────────────────────────────────────


async def test_reconcile_kpis_all_match():
    """All KPIs should be reported as matched."""
    uc = ReconcileKPIsUseCase()
    result = await uc.execute([
        {"name": "revenue", "expected": "100"},
        {"name": "users", "expected": "5000"},
    ])

    assert result.total_kpis == 2
    assert result.matched == 2
    assert result.mismatched == 0


async def test_reconcile_kpis_returns_correct_counts():
    """The total_kpis count should equal the number of KPI definitions."""
    uc = ReconcileKPIsUseCase()
    result = await uc.execute([{"name": "kpi-1"}])

    assert result.total_kpis == 1
    assert result.matched == 1
    assert len(result.details) == 1
    assert result.details[0]["status"] == "matched"


async def test_reconcile_kpis_handles_empty_list():
    """An empty KPI list should produce zero totals."""
    uc = ReconcileKPIsUseCase()
    result = await uc.execute([])

    assert result.total_kpis == 0
    assert result.matched == 0
    assert result.mismatched == 0
    assert result.details == []
