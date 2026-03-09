from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class HealthStatus(Enum):
    HEALTHY = "HEALTHY"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class DeltaLogHealthResult:
    table_name: str
    log_size_mb: float
    status: HealthStatus
    recommendation: str


class DeltaLogHealthService:
    """Pure domain service that evaluates Delta log health for migration readiness."""

    def check_health(
        self, table_name: str, log_size_mb: float
    ) -> DeltaLogHealthResult:
        """Assess Delta log health based on log size in MB."""
        if log_size_mb >= 10.0:
            return DeltaLogHealthResult(
                table_name=table_name,
                log_size_mb=log_size_mb,
                status=HealthStatus.BLOCKED,
                recommendation="Must compact Delta log before migration",
            )

        if log_size_mb >= 9.0:
            return DeltaLogHealthResult(
                table_name=table_name,
                log_size_mb=log_size_mb,
                status=HealthStatus.CRITICAL,
                recommendation="Run OPTIMIZE followed by VACUUM",
            )

        if log_size_mb >= 7.0:
            return DeltaLogHealthResult(
                table_name=table_name,
                log_size_mb=log_size_mb,
                status=HealthStatus.WARNING,
                recommendation="Run OPTIMIZE to reduce log size",
            )

        return DeltaLogHealthResult(
            table_name=table_name,
            log_size_mb=log_size_mb,
            status=HealthStatus.HEALTHY,
            recommendation="No action needed",
        )
