from __future__ import annotations

from athenaforge.application.dtos.wave_dtos import WaveGateResult
from athenaforge.domain.events.wave_events import WaveGateFailed, WaveGatePassed
from athenaforge.domain.ports.event_bus import EventBusPort

_REQUIRED_CRITERIA = (
    "dvt_passed",
    "latency_ok",
    "no_data_loss",
    "streaming_stable",
    "dashboards_verified",
    "kpis_reconciled",
)


class EnforceWaveGateUseCase:
    """Enforce the quality gate for a wave by checking all required criteria."""

    def __init__(self, event_bus: EventBusPort) -> None:
        self._event_bus = event_bus

    async def execute(
        self,
        wave_id: str,
        criteria: dict[str, bool],
    ) -> WaveGateResult:
        criteria_met: list[str] = []
        criteria_failed: list[str] = []

        for name in _REQUIRED_CRITERIA:
            if criteria.get(name, False):
                criteria_met.append(name)
            else:
                criteria_failed.append(name)

        passed = len(criteria_failed) == 0

        if passed:
            await self._event_bus.publish(
                WaveGatePassed(
                    aggregate_id=wave_id,
                    wave_id=wave_id,
                    criteria_met=tuple(criteria_met),
                )
            )
        else:
            await self._event_bus.publish(
                WaveGateFailed(
                    aggregate_id=wave_id,
                    wave_id=wave_id,
                    criteria_failed=tuple(criteria_failed),
                )
            )

        return WaveGateResult(
            wave_id=wave_id,
            passed=passed,
            criteria_met=criteria_met,
            criteria_failed=criteria_failed,
        )
