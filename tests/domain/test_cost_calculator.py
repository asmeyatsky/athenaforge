from __future__ import annotations

from decimal import Decimal

import pytest

from athenaforge.domain.services.cost_calculator import (
    EgressCostCalculator,
    SlotPricingCalculator,
)
from athenaforge.domain.value_objects.cost import Currency

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_GB = 1_073_741_824  # bytes in 1 GiB


@pytest.fixture()
def egress() -> EgressCostCalculator:
    return EgressCostCalculator()


@pytest.fixture()
def slot() -> SlotPricingCalculator:
    return SlotPricingCalculator()


# ---------------------------------------------------------------------------
# EgressCostCalculator tests
# ---------------------------------------------------------------------------


class TestEgressCostFreeOnly:
    """<= 1 GB — entirely free."""

    def test_cost_for_half_gb(self, egress: EgressCostCalculator) -> None:
        size = _GB // 2
        result = egress.calculate_egress_cost(size)
        assert result.amount == Decimal(0)
        assert result.currency == Currency.USD

    def test_cost_for_exactly_1gb(self, egress: EgressCostCalculator) -> None:
        result = egress.calculate_egress_cost(_GB)
        assert result.amount == Decimal(0)


class TestEgressCostSmallData:
    """5 GB — entirely in the first pricing tier."""

    def test_cost_for_5gb(self, egress: EgressCostCalculator) -> None:
        size = 5 * _GB
        result = egress.calculate_egress_cost(size)
        # First 1 GB free, next 4 GB at $0.109/GB → $0.436
        expected = Decimal(4) * Decimal("0.109")
        assert result.amount == expected
        assert result.currency == Currency.USD


class TestEgressCost10TB:
    """10 TB — spans first two tiers."""

    def test_cost_for_10tb(self, egress: EgressCostCalculator) -> None:
        size = 10 * 1024 * _GB  # 10 TB = 10_240 GB
        result = egress.calculate_egress_cost(size)
        # First 1 GB free
        # Next 10_239 GB at $0.109/GB  (all within tier-1 limit of 10_239)
        expected = Decimal(10_239) * Decimal("0.109")
        assert result.amount == expected
        assert result.currency == Currency.USD


class TestEgressCost50TB:
    """50 TB — spans three tiers."""

    def test_cost_for_50tb(self, egress: EgressCostCalculator) -> None:
        size = 50 * 1024 * _GB  # 51_200 GB
        result = egress.calculate_egress_cost(size)
        # 1 GB free
        # Tier 1: 10_239 GB at $0.109
        # Tier 2: 40_960 GB at $0.085 (51_200 - 10_240 = 40_960)
        tier1 = Decimal(10_239) * Decimal("0.109")
        tier2 = Decimal(40_960) * Decimal("0.085")
        expected = tier1 + tier2
        assert result.amount == expected


class TestEgressCost150TB:
    """150 TB — spans exactly three tiers (hits line 73 early return)."""

    def test_cost_for_150tb(self, egress: EgressCostCalculator) -> None:
        size = 150 * 1024 * _GB  # 153_600 GB
        result = egress.calculate_egress_cost(size)
        tier1 = Decimal(10_239) * Decimal("0.109")
        tier2 = Decimal(40_960) * Decimal("0.085")
        tier3 = Decimal(102_400) * Decimal("0.082")
        expected = tier1 + tier2 + tier3
        assert result.amount == expected


class TestEgressCost11PB:
    """11 PB — result should be in $580K–$980K range."""

    def test_cost_for_11pb(self, egress: EgressCostCalculator) -> None:
        size = 11_000_000 * _GB  # 11 PB as ~11M GiB
        result = egress.calculate_egress_cost(size)
        amount_float = float(result.amount)
        assert 580_000 <= amount_float <= 980_000


class TestEgressWithCredits:
    """Verify credit percentage is applied correctly."""

    def test_credit_percentage_applied(self, egress: EgressCostCalculator) -> None:
        size = 5 * _GB
        estimate = egress.calculate_with_credits(size, Decimal("20"))
        base = egress.calculate_egress_cost(size)

        assert estimate.base_cost == base
        assert estimate.credit_percentage == Decimal("20")
        expected_after = base.amount - base.amount * Decimal("20") / Decimal("100")
        assert estimate.with_credits.amount == expected_after
        assert estimate.with_credits.currency == Currency.USD
        assert estimate.description == "AWS Mumbai egress cost"


# ---------------------------------------------------------------------------
# SlotPricingCalculator tests
# ---------------------------------------------------------------------------


class TestSlotPricing1Year:
    def test_100_slots_1_year(self, slot: SlotPricingCalculator) -> None:
        res = slot.calculate_monthly_cost(slots=100, commitment_years=1)
        # 100 * $0.04/slot/hr * 720 hrs = $2,880/mo
        assert res.monthly_cost.amount == Decimal(100) * Decimal("0.04") * Decimal(720)
        assert res.monthly_cost.currency == Currency.USD
        assert res.edition == "Enterprise"
        assert res.commitment_years == 1
        assert res.slots == 100


class TestSlotPricing3Year:
    def test_100_slots_3_year(self, slot: SlotPricingCalculator) -> None:
        res = slot.calculate_monthly_cost(slots=100, commitment_years=3)
        expected = Decimal(100) * Decimal("0.032") * Decimal(720)
        assert res.monthly_cost.amount == expected
        assert res.commitment_years == 3


class TestSlotPricingDifferentSlotCounts:
    @pytest.mark.parametrize("slots", [50, 200, 500])
    def test_various_slot_counts(
        self, slot: SlotPricingCalculator, slots: int
    ) -> None:
        res = slot.calculate_monthly_cost(slots=slots, commitment_years=1)
        expected = Decimal(slots) * Decimal("0.04") * Decimal(720)
        assert res.monthly_cost.amount == expected
        assert res.slots == slots


class TestSlotPricingInvalidTerm:
    def test_unsupported_commitment_raises(self, slot: SlotPricingCalculator) -> None:
        with pytest.raises(ValueError, match="Unsupported commitment term"):
            slot.calculate_monthly_cost(slots=100, commitment_years=2)
