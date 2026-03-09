from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any, Callable

from athenaforge.domain.events.event_base import DomainEvent
from athenaforge.domain.ports.event_bus import EventBusPort


class InMemoryEventBus:
    """In-process async event bus for domain events."""

    def __init__(self) -> None:
        self._handlers: dict[type, list[Callable]] = defaultdict(list)
        self._published: list[DomainEvent] = []

    async def publish(self, event: DomainEvent) -> None:
        self._published.append(event)
        event_type = type(event)
        for handler in self._handlers.get(event_type, []):
            if asyncio.iscoroutinefunction(handler):
                await handler(event)
            else:
                handler(event)

    def subscribe(self, event_type: type, handler: Callable) -> None:
        self._handlers[event_type].append(handler)

    @property
    def published_events(self) -> list[DomainEvent]:
        return list(self._published)
