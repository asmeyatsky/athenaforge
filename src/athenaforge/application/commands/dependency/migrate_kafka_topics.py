from __future__ import annotations

from athenaforge.application.dtos.dependency_dtos import KafkaMigrationReport
from athenaforge.domain.events.dependency_events import KafkaTopicMigrated
from athenaforge.domain.ports.event_bus import EventBusPort


class MigrateKafkaTopicsUseCase:
    """Migrate Kafka topic configurations (placeholder implementation)."""

    def __init__(self, event_bus: EventBusPort) -> None:
        self._event_bus = event_bus

    async def execute(
        self, topic_configs: list[dict[str, str]]
    ) -> KafkaMigrationReport:
        topics_migrated = 0
        schemas_updated = 0
        details: list[dict[str, str]] = []

        for config in topic_configs:
            topic_name = config.get("topic", "unknown")
            topics_migrated += 1

            # If the config includes a schema reference, count it as updated
            if "schema" in config:
                schemas_updated += 1

            details.append(
                {"topic": topic_name, "status": "migrated"}
            )

        await self._event_bus.publish(
            KafkaTopicMigrated(
                aggregate_id="kafka-migration",
                topics_migrated=topics_migrated,
                schemas_updated=schemas_updated,
            )
        )

        return KafkaMigrationReport(
            topics_migrated=topics_migrated,
            schemas_updated=schemas_updated,
            details=details,
        )
