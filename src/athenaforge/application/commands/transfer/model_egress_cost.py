from __future__ import annotations

from decimal import Decimal

from athenaforge.application.dtos.transfer_dtos import EgressCostReport
from athenaforge.domain.ports.event_bus import EventBusPort
from athenaforge.domain.services.cost_calculator import EgressCostCalculator

_COMPRESSION_FACTOR = Decimal("0.70")  # 30% reduction


class ModelEgressCostUseCase:
    """Model data-egress costs across multiple scenarios."""

    def __init__(
        self,
        cost_calculator: EgressCostCalculator,
        event_bus: EventBusPort,
    ) -> None:
        self._cost_calculator = cost_calculator
        self._event_bus = event_bus

    async def execute(
        self, total_size_bytes: int, credit_percentage: float = 0.0
    ) -> EgressCostReport:
        if total_size_bytes < 0:
            raise ValueError("total_size_bytes must be >= 0")

        base_cost = self._cost_calculator.calculate_egress_cost(total_size_bytes)

        credit_pct = Decimal(str(credit_percentage))
        estimate = self._cost_calculator.calculate_with_credits(
            total_size_bytes, credit_pct
        )

        optimized_bytes = int(Decimal(total_size_bytes) * _COMPRESSION_FACTOR)
        optimized_estimate = self._cost_calculator.calculate_with_credits(
            optimized_bytes, credit_pct
        )

        return EgressCostReport(
            total_size_bytes=total_size_bytes,
            scenario_base_usd=float(base_cost.amount),
            scenario_with_credits_usd=float(estimate.with_credits.amount),
            scenario_optimized_usd=float(optimized_estimate.with_credits.amount),
            credit_percentage=credit_percentage,
        )
