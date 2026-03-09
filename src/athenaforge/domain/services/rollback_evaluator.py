from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RollbackCondition:
    name: str
    triggered: bool
    details: str


class RollbackEvaluator:
    """Pure domain service that decides whether a rollback is warranted."""

    def evaluate(
        self,
        dvt_pass_rate: float,
        latency_increase_pct: float,
        data_loss_detected: bool,
        streaming_lag: int,
        escalation_raised: bool,
        lag_threshold: int = 1000,
    ) -> tuple[bool, list[RollbackCondition]]:
        """Evaluate rollback conditions.

        Returns ``(should_rollback, conditions)`` where *should_rollback*
        is ``True`` when **any** condition is triggered.
        """
        conditions: list[RollbackCondition] = []

        dvt_triggered = dvt_pass_rate < 0.99
        conditions.append(
            RollbackCondition(
                name="DVT pass rate",
                triggered=dvt_triggered,
                details=f"Pass rate {dvt_pass_rate:.4f} ({'below' if dvt_triggered else 'at or above'} 0.99 threshold)",
            )
        )

        latency_triggered = latency_increase_pct > 20.0
        conditions.append(
            RollbackCondition(
                name="Latency increase",
                triggered=latency_triggered,
                details=f"Latency increase {latency_increase_pct:.1f}% ({'exceeds' if latency_triggered else 'within'} 20% threshold)",
            )
        )

        conditions.append(
            RollbackCondition(
                name="Data loss",
                triggered=data_loss_detected,
                details="Data loss detected" if data_loss_detected else "No data loss detected",
            )
        )

        lag_triggered = streaming_lag > lag_threshold
        conditions.append(
            RollbackCondition(
                name="Streaming lag",
                triggered=lag_triggered,
                details=f"Streaming lag {streaming_lag} ({'exceeds' if lag_triggered else 'within'} {lag_threshold} threshold)",
            )
        )

        conditions.append(
            RollbackCondition(
                name="Escalation raised",
                triggered=escalation_raised,
                details="Escalation raised" if escalation_raised else "No escalation raised",
            )
        )

        should_rollback = any(c.triggered for c in conditions)
        return should_rollback, conditions
