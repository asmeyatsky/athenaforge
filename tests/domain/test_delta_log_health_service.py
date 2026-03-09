"""Tests for DeltaLogHealthService — pure domain logic, no mocks."""
from __future__ import annotations

import pytest

from athenaforge.domain.services.delta_log_health_service import (
    DeltaLogHealthResult,
    DeltaLogHealthService,
    HealthStatus,
)


class TestHealthyStatus:
    def test_5mb_is_healthy(self):
        svc = DeltaLogHealthService()
        result = svc.check_health("orders", 5.0)

        assert result.status == HealthStatus.HEALTHY
        assert result.table_name == "orders"
        assert result.log_size_mb == 5.0
        assert result.recommendation == "No action needed"

    def test_0mb_is_healthy(self):
        svc = DeltaLogHealthService()
        result = svc.check_health("empty_table", 0.0)

        assert result.status == HealthStatus.HEALTHY

    def test_6_9mb_is_healthy(self):
        svc = DeltaLogHealthService()
        result = svc.check_health("table", 6.9)

        assert result.status == HealthStatus.HEALTHY


class TestWarningStatus:
    def test_7mb_boundary_is_warning(self):
        svc = DeltaLogHealthService()
        result = svc.check_health("logs", 7.0)

        assert result.status == HealthStatus.WARNING
        assert result.log_size_mb == 7.0
        assert "OPTIMIZE" in result.recommendation

    def test_8mb_is_warning(self):
        svc = DeltaLogHealthService()
        result = svc.check_health("events", 8.0)

        assert result.status == HealthStatus.WARNING

    def test_8_9mb_is_warning(self):
        svc = DeltaLogHealthService()
        result = svc.check_health("table", 8.9)

        assert result.status == HealthStatus.WARNING


class TestCriticalStatus:
    def test_9mb_boundary_is_critical(self):
        svc = DeltaLogHealthService()
        result = svc.check_health("big_table", 9.0)

        assert result.status == HealthStatus.CRITICAL
        assert result.log_size_mb == 9.0
        assert "OPTIMIZE" in result.recommendation
        assert "VACUUM" in result.recommendation

    def test_9_5mb_is_critical(self):
        svc = DeltaLogHealthService()
        result = svc.check_health("growing", 9.5)

        assert result.status == HealthStatus.CRITICAL

    def test_9_9mb_is_critical(self):
        svc = DeltaLogHealthService()
        result = svc.check_health("table", 9.9)

        assert result.status == HealthStatus.CRITICAL


class TestBlockedStatus:
    def test_10mb_boundary_is_blocked(self):
        svc = DeltaLogHealthService()
        result = svc.check_health("huge_table", 10.0)

        assert result.status == HealthStatus.BLOCKED
        assert result.log_size_mb == 10.0
        assert "compact" in result.recommendation.lower()

    def test_15mb_is_blocked(self):
        svc = DeltaLogHealthService()
        result = svc.check_health("massive", 15.0)

        assert result.status == HealthStatus.BLOCKED

    def test_100mb_is_blocked(self):
        svc = DeltaLogHealthService()
        result = svc.check_health("extreme", 100.0)

        assert result.status == HealthStatus.BLOCKED


class TestRecommendations:
    def test_healthy_recommendation(self):
        svc = DeltaLogHealthService()
        result = svc.check_health("t", 1.0)

        assert result.recommendation == "No action needed"

    def test_warning_recommendation(self):
        svc = DeltaLogHealthService()
        result = svc.check_health("t", 7.5)

        assert result.recommendation == "Run OPTIMIZE to reduce log size"

    def test_critical_recommendation(self):
        svc = DeltaLogHealthService()
        result = svc.check_health("t", 9.2)

        assert result.recommendation == "Run OPTIMIZE followed by VACUUM"

    def test_blocked_recommendation(self):
        svc = DeltaLogHealthService()
        result = svc.check_health("t", 12.0)

        assert result.recommendation == "Must compact Delta log before migration"


class TestResultStructure:
    def test_result_is_frozen(self):
        svc = DeltaLogHealthService()
        result = svc.check_health("t", 5.0)

        with pytest.raises(AttributeError):
            result.status = HealthStatus.BLOCKED  # type: ignore[misc]

    def test_result_fields(self):
        svc = DeltaLogHealthService()
        result = svc.check_health("my_table", 3.0)

        assert isinstance(result, DeltaLogHealthResult)
        assert result.table_name == "my_table"
        assert result.log_size_mb == 3.0
        assert isinstance(result.status, HealthStatus)
        assert isinstance(result.recommendation, str)
