from __future__ import annotations

from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from athenaforge.domain.value_objects.cost import Currency, Money
from athenaforge.domain.value_objects.sql_pattern import PatternCategory
from athenaforge.domain.value_objects.tier import Tier, TierClassification
from athenaforge.domain.value_objects.validation_result import (
    Severity,
    ValidationIssue,
    ValidationResult,
)
from athenaforge.domain.value_objects.wave import WaveStatus

# ---------------------------------------------------------------------------
# Money
# ---------------------------------------------------------------------------


class TestMoneyArithmetic:
    def test_add(self) -> None:
        a = Money(amount=Decimal("10.50"), currency=Currency.USD)
        b = Money(amount=Decimal("4.50"), currency=Currency.USD)
        result = a + b
        assert result.amount == Decimal("15.00")
        assert result.currency == Currency.USD

    def test_subtract(self) -> None:
        a = Money(amount=Decimal("20.00"), currency=Currency.USD)
        b = Money(amount=Decimal("7.50"), currency=Currency.USD)
        result = a - b
        assert result.amount == Decimal("12.50")
        assert result.currency == Currency.USD

    def test_multiply_by_int(self) -> None:
        m = Money(amount=Decimal("5.00"), currency=Currency.USD)
        result = m * 3
        assert result.amount == Decimal("15.00")

    def test_multiply_by_float(self) -> None:
        m = Money(amount=Decimal("10.00"), currency=Currency.USD)
        result = m * 0.5
        assert result.amount == Decimal("5.00")

    def test_multiply_by_decimal(self) -> None:
        m = Money(amount=Decimal("10.00"), currency=Currency.USD)
        result = m * Decimal("2.5")
        assert result.amount == Decimal("25.00")

    def test_rmul(self) -> None:
        m = Money(amount=Decimal("8.00"), currency=Currency.USD)
        result = 2 * m
        assert result.amount == Decimal("16.00")

    def test_currency_mismatch_add_raises(self) -> None:
        usd = Money(amount=Decimal("10"), currency=Currency.USD)
        inr = Money(amount=Decimal("10"), currency=Currency.INR)
        with pytest.raises(ValueError, match="Currency mismatch"):
            usd + inr

    def test_currency_mismatch_sub_raises(self) -> None:
        usd = Money(amount=Decimal("10"), currency=Currency.USD)
        inr = Money(amount=Decimal("10"), currency=Currency.INR)
        with pytest.raises(ValueError, match="Currency mismatch"):
            usd - inr


# ---------------------------------------------------------------------------
# Tier
# ---------------------------------------------------------------------------


class TestTier:
    def test_enum_values(self) -> None:
        assert Tier.TIER_1.value == "TIER_1"
        assert Tier.TIER_2.value == "TIER_2"
        assert Tier.TIER_3.value == "TIER_3"

    def test_all_members(self) -> None:
        members = {t.name for t in Tier}
        assert members == {"TIER_1", "TIER_2", "TIER_3"}


# ---------------------------------------------------------------------------
# TierClassification
# ---------------------------------------------------------------------------


class TestTierClassification:
    def test_frozen(self) -> None:
        tc = TierClassification(
            table_name="t1",
            tier=Tier.TIER_1,
            reason="test",
            size_bytes=100,
            last_queried_days_ago=5,
        )
        with pytest.raises(FrozenInstanceError):
            tc.tier = Tier.TIER_2  # type: ignore[misc]


# ---------------------------------------------------------------------------
# WaveStatus
# ---------------------------------------------------------------------------


class TestWaveStatus:
    def test_enum_values(self) -> None:
        assert WaveStatus.PLANNED.value == "PLANNED"
        assert WaveStatus.SHADOW_RUNNING.value == "SHADOW_RUNNING"
        assert WaveStatus.REVERSE_SHADOW_RUNNING.value == "REVERSE_SHADOW_RUNNING"
        assert WaveStatus.CUTOVER_READY.value == "CUTOVER_READY"
        assert WaveStatus.CUTTING_OVER.value == "CUTTING_OVER"
        assert WaveStatus.COMPLETED.value == "COMPLETED"
        assert WaveStatus.ROLLED_BACK.value == "ROLLED_BACK"

    def test_all_members(self) -> None:
        assert len(WaveStatus) == 7


# ---------------------------------------------------------------------------
# PatternCategory
# ---------------------------------------------------------------------------


class TestPatternCategory:
    def test_enum_values(self) -> None:
        assert PatternCategory.MAP_CONSTRUCTOR.value == "MAP_CONSTRUCTOR"
        assert PatternCategory.TRANSFORM.value == "TRANSFORM"
        assert PatternCategory.FILTER.value == "FILTER"
        assert PatternCategory.DATE_TRUNC.value == "DATE_TRUNC"
        assert PatternCategory.DATE_ADD.value == "DATE_ADD"
        assert PatternCategory.TRY_CAST.value == "TRY_CAST"
        assert PatternCategory.REGEXP_LIKE.value == "REGEXP_LIKE"
        assert PatternCategory.APPROX_DISTINCT.value == "APPROX_DISTINCT"
        assert PatternCategory.ARBITRARY.value == "ARBITRARY"
        assert PatternCategory.CONTAINS.value == "CONTAINS"
        assert PatternCategory.MAP_KEYS.value == "MAP_KEYS"
        assert PatternCategory.MAP_VALUES.value == "MAP_VALUES"
        assert PatternCategory.UDF.value == "UDF"
        assert PatternCategory.OTHER.value == "OTHER"

    def test_all_members(self) -> None:
        assert len(PatternCategory) == 30


# ---------------------------------------------------------------------------
# ValidationResult
# ---------------------------------------------------------------------------


class TestValidationResult:
    def test_passed_count(self) -> None:
        issues = (
            ValidationIssue(severity=Severity.INFO, code="I1", message="info"),
            ValidationIssue(severity=Severity.WARNING, code="W1", message="warn"),
            ValidationIssue(severity=Severity.ERROR, code="E1", message="err"),
            ValidationIssue(severity=Severity.ERROR, code="E2", message="err2"),
        )
        vr = ValidationResult(is_valid=False, issues=issues)
        assert vr.passed_count == 2  # INFO + WARNING

    def test_failed_count(self) -> None:
        issues = (
            ValidationIssue(severity=Severity.INFO, code="I1", message="info"),
            ValidationIssue(severity=Severity.ERROR, code="E1", message="err"),
            ValidationIssue(severity=Severity.ERROR, code="E2", message="err2"),
        )
        vr = ValidationResult(is_valid=False, issues=issues)
        assert vr.failed_count == 2

    def test_no_issues(self) -> None:
        vr = ValidationResult(is_valid=True, issues=())
        assert vr.passed_count == 0
        assert vr.failed_count == 0
