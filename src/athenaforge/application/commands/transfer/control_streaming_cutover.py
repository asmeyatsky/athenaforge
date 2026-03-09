from __future__ import annotations

from athenaforge.application.dtos.transfer_dtos import StreamingCutoverResult
from athenaforge.domain.entities.streaming_job import StreamingJob
from athenaforge.domain.ports.event_bus import EventBusPort


class ControlStreamingCutoverUseCase:
    """Orchestrate a streaming pipeline cutover from source to target topic."""

    def __init__(
        self,
        event_bus: EventBusPort,
    ) -> None:
        self._event_bus = event_bus

    async def execute(
        self,
        job_id: str,
        source_topic: str,
        target_topic: str,
        current_lag: int = 0,
    ) -> StreamingCutoverResult:
        job = StreamingJob(
            job_id=job_id,
            source_topic=source_topic,
            target_topic=target_topic,
            consumer_group=f"{job_id}-consumer-group",
            current_lag=current_lag,
        )

        job = job.initiate_drain()
        job = job.switch_target()

        for event in job.collect_events():
            await self._event_bus.publish(event)

        return StreamingCutoverResult(
            job_id=job_id,
            source_topic=source_topic,
            target_topic=target_topic,
            status=job.status,
            lag_at_cutover=current_lag,
        )
