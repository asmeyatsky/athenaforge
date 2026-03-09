"""Tests for RollbackEvaluator — pure domain logic, no mocks."""
from __future__ import annotations

import pytest

from athenaforge.domain.services.rollback_evaluator import (
    RollbackCondition,
    RollbackEvaluator,
)

# Healthy defaults — no trigger expected when all are used
_HEALTHY_DEFAULTS = dict(
    dvt_pass_rate=1.0,
    latency_increase_pct=0.0,
    data_loss_detected=False,
    streaming_lag=0,
    escalation_raised=False,
)


class TestDvtPassRateTrigger:
    def test_below_threshold_triggers_rollback(self):
        evaluator = RollbackEvaluator()
        should_rollback, conditions = evaluator.evaluate(
            **{**_HEALTHY_DEFAULTS, "dvt_pass_rate": 0.98}
        )

        assert should_rollback is True
        dvt_cond = next(c for c in conditions if c.name == "DVT pass rate")
        assert dvt_cond.triggered is True

    def test_at_threshold_no_trigger(self):
        evaluator = RollbackEvaluator()
        should_rollback, conditions = evaluator.evaluate(
            **{**_HEALTHY_DEFAULTS, "dvt_pass_rate": 0.99}
        )

        dvt_cond = next(c for c in conditions if c.name == "DVT pass rate")
        assert dvt_cond.triggered is False
        # All other conditions are healthy, so no rollback
        assert should_rollback is False

    def test_above_threshold_no_trigger(self):
        evaluator = RollbackEvaluator()
        should_rollback, conditions = evaluator.evaluate(
            **{**_HEALTHY_DEFAULTS, "dvt_pass_rate": 1.0}
        )

        dvt_cond = next(c for c in conditions if c.name == "DVT pass rate")
        assert dvt_cond.triggered is False


class TestLatencyIncreaseTrigger:
    def test_above_threshold_triggers_rollback(self):
        evaluator = RollbackEvaluator()
        should_rollback, conditions = evaluator.evaluate(
            **{**_HEALTHY_DEFAULTS, "latency_increase_pct": 25.0}
        )

        assert should_rollback is True
        latency_cond = next(c for c in conditions if c.name == "Latency increase")
        assert latency_cond.triggered is True

    def test_at_threshold_no_trigger(self):
        evaluator = RollbackEvaluator()
        should_rollback, conditions = evaluator.evaluate(
            **{**_HEALTHY_DEFAULTS, "latency_increase_pct": 20.0}
        )

        latency_cond = next(c for c in conditions if c.name == "Latency increase")
        assert latency_cond.triggered is False
        assert should_rollback is False

    def test_below_threshold_no_trigger(self):
        evaluator = RollbackEvaluator()
        should_rollback, conditions = evaluator.evaluate(
            **{**_HEALTHY_DEFAULTS, "latency_increase_pct": 10.0}
        )

        latency_cond = next(c for c in conditions if c.name == "Latency increase")
        assert latency_cond.triggered is False


class TestDataLossTrigger:
    def test_data_loss_detected_triggers_rollback(self):
        evaluator = RollbackEvaluator()
        should_rollback, conditions = evaluator.evaluate(
            **{**_HEALTHY_DEFAULTS, "data_loss_detected": True}
        )

        assert should_rollback is True
        dl_cond = next(c for c in conditions if c.name == "Data loss")
        assert dl_cond.triggered is True

    def test_no_data_loss_no_trigger(self):
        evaluator = RollbackEvaluator()
        should_rollback, conditions = evaluator.evaluate(
            **{**_HEALTHY_DEFAULTS, "data_loss_detected": False}
        )

        dl_cond = next(c for c in conditions if c.name == "Data loss")
        assert dl_cond.triggered is False


class TestStreamingLagTrigger:
    def test_above_threshold_triggers_rollback(self):
        evaluator = RollbackEvaluator()
        should_rollback, conditions = evaluator.evaluate(
            **{**_HEALTHY_DEFAULTS, "streaming_lag": 1500}
        )

        assert should_rollback is True
        lag_cond = next(c for c in conditions if c.name == "Streaming lag")
        assert lag_cond.triggered is True

    def test_at_threshold_no_trigger(self):
        evaluator = RollbackEvaluator()
        should_rollback, conditions = evaluator.evaluate(
            **{**_HEALTHY_DEFAULTS, "streaming_lag": 1000}
        )

        lag_cond = next(c for c in conditions if c.name == "Streaming lag")
        assert lag_cond.triggered is False
        assert should_rollback is False

    def test_below_threshold_no_trigger(self):
        evaluator = RollbackEvaluator()
        should_rollback, conditions = evaluator.evaluate(
            **{**_HEALTHY_DEFAULTS, "streaming_lag": 500}
        )

        lag_cond = next(c for c in conditions if c.name == "Streaming lag")
        assert lag_cond.triggered is False


class TestEscalationRaisedTrigger:
    def test_escalation_triggers_rollback(self):
        evaluator = RollbackEvaluator()
        should_rollback, conditions = evaluator.evaluate(
            **{**_HEALTHY_DEFAULTS, "escalation_raised": True}
        )

        assert should_rollback is True
        esc_cond = next(c for c in conditions if c.name == "Escalation raised")
        assert esc_cond.triggered is True

    def test_no_escalation_no_trigger(self):
        evaluator = RollbackEvaluator()
        should_rollback, conditions = evaluator.evaluate(
            **{**_HEALTHY_DEFAULTS, "escalation_raised": False}
        )

        esc_cond = next(c for c in conditions if c.name == "Escalation raised")
        assert esc_cond.triggered is False


class TestAllHealthy:
    def test_all_healthy_no_rollback(self):
        evaluator = RollbackEvaluator()
        should_rollback, conditions = evaluator.evaluate(**_HEALTHY_DEFAULTS)

        assert should_rollback is False
        assert all(c.triggered is False for c in conditions)

    def test_all_conditions_present(self):
        evaluator = RollbackEvaluator()
        _, conditions = evaluator.evaluate(**_HEALTHY_DEFAULTS)

        names = {c.name for c in conditions}
        assert names == {
            "DVT pass rate",
            "Latency increase",
            "Data loss",
            "Streaming lag",
            "Escalation raised",
        }


class TestMultipleConditionsTriggered:
    def test_two_conditions_triggered(self):
        evaluator = RollbackEvaluator()
        should_rollback, conditions = evaluator.evaluate(
            dvt_pass_rate=0.95,
            latency_increase_pct=30.0,
            data_loss_detected=False,
            streaming_lag=0,
            escalation_raised=False,
        )

        assert should_rollback is True
        triggered = [c for c in conditions if c.triggered]
        assert len(triggered) == 2

    def test_all_conditions_triggered(self):
        evaluator = RollbackEvaluator()
        should_rollback, conditions = evaluator.evaluate(
            dvt_pass_rate=0.50,
            latency_increase_pct=100.0,
            data_loss_detected=True,
            streaming_lag=5000,
            escalation_raised=True,
        )

        assert should_rollback is True
        assert all(c.triggered is True for c in conditions)


class TestBoundaryValues:
    def test_dvt_just_below_threshold(self):
        evaluator = RollbackEvaluator()
        should_rollback, _ = evaluator.evaluate(
            **{**_HEALTHY_DEFAULTS, "dvt_pass_rate": 0.9899999}
        )

        assert should_rollback is True

    def test_latency_just_above_threshold(self):
        evaluator = RollbackEvaluator()
        should_rollback, conditions = evaluator.evaluate(
            **{**_HEALTHY_DEFAULTS, "latency_increase_pct": 20.0001}
        )

        assert should_rollback is True
        latency_cond = next(c for c in conditions if c.name == "Latency increase")
        assert latency_cond.triggered is True

    def test_streaming_lag_just_above_threshold(self):
        evaluator = RollbackEvaluator()
        should_rollback, conditions = evaluator.evaluate(
            **{**_HEALTHY_DEFAULTS, "streaming_lag": 1001}
        )

        assert should_rollback is True
        lag_cond = next(c for c in conditions if c.name == "Streaming lag")
        assert lag_cond.triggered is True
