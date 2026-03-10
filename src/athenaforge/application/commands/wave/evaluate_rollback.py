from __future__ import annotations

from athenaforge.application.dtos.wave_dtos import RollbackCheckResult
from athenaforge.domain.events.wave_events import RollbackTriggered
from athenaforge.domain.ports.event_bus import EventBusPort
from athenaforge.domain.services.rollback_evaluator import RollbackEvaluator


class EvaluateRollbackUseCase:
    """Evaluate whether a wave should be rolled back based on operational metrics."""

    def __init__(
        self,
        evaluator: RollbackEvaluator,
        event_bus: EventBusPort,
    ) -> None:
        self._evaluator = evaluator
        self._event_bus = event_bus

    async def execute(
        self,
        wave_id: str,
        dvt_pass_rate: float,
        latency_increase_pct: float,
        data_loss_detected: bool,
        streaming_lag: int,
        escalation_raised: bool,
    ) -> RollbackCheckResult:
        if not (0.0 <= dvt_pass_rate <= 1.0):
            raise ValueError("dvt_pass_rate must be between 0.0 and 1.0")
        if latency_increase_pct < 0.0:
            raise ValueError("latency_increase_pct must be >= 0.0")
        if streaming_lag < 0:
            raise ValueError("streaming_lag must be >= 0")

        should_rollback, conditions = self._evaluator.evaluate(
            dvt_pass_rate=dvt_pass_rate,
            latency_increase_pct=latency_increase_pct,
            data_loss_detected=data_loss_detected,
            streaming_lag=streaming_lag,
            escalation_raised=escalation_raised,
        )

        if should_rollback:
            triggered_names = [c.name for c in conditions if c.triggered]
            await self._event_bus.publish(
                RollbackTriggered(
                    aggregate_id=wave_id,
                    wave_id=wave_id,
                    reason=f"Triggered conditions: {', '.join(triggered_names)}",
                )
            )

        return RollbackCheckResult(
            should_rollback=should_rollback,
            conditions=[
                {
                    "name": c.name,
                    "triggered": c.triggered,
                    "details": c.details,
                }
                for c in conditions
            ],
        )
