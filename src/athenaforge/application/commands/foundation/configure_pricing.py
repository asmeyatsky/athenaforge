from __future__ import annotations

import os

from athenaforge.application.dtos.foundation_dtos import PricingResult
from athenaforge.domain.ports.event_bus import EventBusPort
from athenaforge.domain.ports.terraform_port import TerraformGeneratorPort
from athenaforge.domain.services.cost_calculator import SlotPricingCalculator


class ConfigurePricingUseCase:
    """Calculate slot pricing and optionally generate a Terraform reservation file."""

    def __init__(
        self,
        pricing_calculator: SlotPricingCalculator,
        terraform_generator: TerraformGeneratorPort,
        event_bus: EventBusPort,
    ) -> None:
        self._pricing_calculator = pricing_calculator
        self._terraform_generator = terraform_generator
        self._event_bus = event_bus

    async def execute(
        self, slots: int, commitment_years: int, output_dir: str
    ) -> PricingResult:
        reservation = self._pricing_calculator.calculate_monthly_cost(
            slots, commitment_years
        )

        monthly_cost_usd = float(reservation.monthly_cost.amount)

        context = {
            "edition": reservation.edition,
            "slots": reservation.slots,
            "commitment_years": reservation.commitment_years,
            "monthly_cost_usd": monthly_cost_usd,
        }
        content = self._terraform_generator.render_template(
            "reservation.tf", context
        )
        terraform_file = os.path.join(output_dir, "reservation.tf")
        self._terraform_generator.write_file(terraform_file, content)

        return PricingResult(
            edition=reservation.edition,
            slots=reservation.slots,
            monthly_cost_usd=monthly_cost_usd,
            terraform_file=terraform_file,
        )
