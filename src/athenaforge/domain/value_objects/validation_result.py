from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Severity(Enum):
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


@dataclass(frozen=True)
class ValidationIssue:
    severity: Severity
    code: str
    message: str
    location: str | None = None


@dataclass(frozen=True)
class ValidationResult:
    is_valid: bool
    issues: tuple[ValidationIssue, ...]

    @property
    def passed_count(self) -> int:
        return sum(
            1 for issue in self.issues if issue.severity != Severity.ERROR
        )

    @property
    def failed_count(self) -> int:
        return sum(
            1 for issue in self.issues if issue.severity == Severity.ERROR
        )
