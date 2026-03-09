from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum


class Currency(Enum):
    USD = "USD"
    INR = "INR"


@dataclass(frozen=True)
class Money:
    amount: Decimal
    currency: Currency

    def _check_currency(self, other: Money) -> None:
        if self.currency != other.currency:
            raise ValueError(
                f"Currency mismatch: {self.currency.value} vs {other.currency.value}"
            )

    def __add__(self, other: Money) -> Money:
        self._check_currency(other)
        return Money(amount=self.amount + other.amount, currency=self.currency)

    def __sub__(self, other: Money) -> Money:
        self._check_currency(other)
        return Money(amount=self.amount - other.amount, currency=self.currency)

    def __mul__(self, scalar: int | float | Decimal) -> Money:
        return Money(amount=self.amount * Decimal(str(scalar)), currency=self.currency)

    def __rmul__(self, scalar: int | float | Decimal) -> Money:
        return self.__mul__(scalar)


@dataclass(frozen=True)
class CostEstimate:
    description: str
    base_cost: Money
    with_credits: Money
    credit_percentage: Decimal


@dataclass(frozen=True)
class SlotReservation:
    edition: str
    commitment_years: int
    slots: int
    monthly_cost: Money
