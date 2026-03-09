from __future__ import annotations

from decimal import Decimal

from athenaforge.domain.value_objects.cost import (
    CostEstimate,
    Currency,
    Money,
    SlotReservation,
)

_GB = 1_073_741_824  # bytes in 1 GiB

# AWS Mumbai egress pricing tiers (cumulative boundaries in GB)
_TIER_FREE = Decimal(1)        # first 1 GB free
_TIER_10TB = Decimal(10_240)   # up to 10 TB
_TIER_50TB = Decimal(51_200)   # next 40 TB  (10 TB + 40 TB)
_TIER_150TB = Decimal(153_600) # next 100 TB (50 TB + 100 TB)

_RATE_FIRST = Decimal("0.109")
_RATE_SECOND = Decimal("0.085")
_RATE_THIRD = Decimal("0.082")
_RATE_FOURTH = Decimal("0.080")

# BigQuery Enterprise slot pricing
_ENTERPRISE_SLOTS = 100
_RATE_1Y = Decimal("0.04")    # $/slot/hr for 1-year commitment
_RATE_3Y = Decimal("0.032")   # $/slot/hr for 3-year commitment
_HOURS_PER_MONTH = Decimal(24 * 30)  # 720


class EgressCostCalculator:
    """Pure domain service for AWS Mumbai data-egress cost estimation."""

    def calculate_egress_cost(self, size_bytes: int) -> Money:
        """Return the total egress cost in USD for *size_bytes*."""
        size_gb = Decimal(size_bytes) / Decimal(_GB)
        cost = Decimal(0)
        remaining = size_gb

        # First 1 GB free
        free_portion = min(remaining, _TIER_FREE)
        remaining -= free_portion

        if remaining <= Decimal(0):
            return Money(amount=cost, currency=Currency.USD)

        # Up to 10 TB (10_240 GB) at $0.109/GB
        tier1_limit = _TIER_10TB - _TIER_FREE
        tier1_usage = min(remaining, tier1_limit)
        cost += tier1_usage * _RATE_FIRST
        remaining -= tier1_usage

        if remaining <= Decimal(0):
            return Money(amount=cost, currency=Currency.USD)

        # Next 40 TB at $0.085/GB
        tier2_limit = _TIER_50TB - _TIER_10TB
        tier2_usage = min(remaining, tier2_limit)
        cost += tier2_usage * _RATE_SECOND
        remaining -= tier2_usage

        if remaining <= Decimal(0):
            return Money(amount=cost, currency=Currency.USD)

        # Next 100 TB at $0.082/GB
        tier3_limit = _TIER_150TB - _TIER_50TB
        tier3_usage = min(remaining, tier3_limit)
        cost += tier3_usage * _RATE_THIRD
        remaining -= tier3_usage

        if remaining <= Decimal(0):
            return Money(amount=cost, currency=Currency.USD)

        # Over 150 TB at $0.080/GB
        cost += remaining * _RATE_FOURTH

        return Money(amount=cost, currency=Currency.USD)

    def calculate_with_credits(
        self, size_bytes: int, credit_pct: Decimal
    ) -> CostEstimate:
        """Return a cost estimate including a credit discount."""
        base = self.calculate_egress_cost(size_bytes)
        discount = base.amount * credit_pct / Decimal(100)
        after_credits = Money(
            amount=base.amount - discount, currency=Currency.USD
        )
        return CostEstimate(
            description="AWS Mumbai egress cost",
            base_cost=base,
            with_credits=after_credits,
            credit_percentage=credit_pct,
        )


class SlotPricingCalculator:
    """Pure domain service for BigQuery Enterprise slot reservation pricing."""

    def calculate_monthly_cost(
        self, slots: int, commitment_years: int
    ) -> SlotReservation:
        """Return a ``SlotReservation`` for the given slot count and term."""
        if commitment_years == 1:
            rate = _RATE_1Y
        elif commitment_years == 3:
            rate = _RATE_3Y
        else:
            raise ValueError(
                f"Unsupported commitment term: {commitment_years} years "
                "(must be 1 or 3)"
            )

        monthly = Decimal(slots) * rate * _HOURS_PER_MONTH
        return SlotReservation(
            edition="Enterprise",
            commitment_years=commitment_years,
            slots=slots,
            monthly_cost=Money(amount=monthly, currency=Currency.USD),
        )
