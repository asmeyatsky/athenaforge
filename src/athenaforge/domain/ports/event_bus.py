from __future__ import annotations

from typing import Callable, Protocol

from athenaforge.domain.events.event_base import DomainEvent


class EventBusPort(Protocol):
    """Port for publishing and subscribing to domain events."""

    async def publish(self, event: DomainEvent) -> None: ...

    def subscribe(self, event_type: type, handler: Callable) -> None: ...
