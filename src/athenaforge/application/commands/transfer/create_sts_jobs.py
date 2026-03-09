from __future__ import annotations

from athenaforge.application.dtos.transfer_dtos import STSJobResult
from athenaforge.domain.events.transfer_events import TransferJobCreated
from athenaforge.domain.ports.event_bus import EventBusPort
from athenaforge.domain.ports.transfer_port import StorageTransferPort


class CreateSTSJobsUseCase:
    """Create Storage Transfer Service jobs for one or more source buckets."""

    def __init__(
        self,
        transfer_port: StorageTransferPort,
        event_bus: EventBusPort,
    ) -> None:
        self._transfer_port = transfer_port
        self._event_bus = event_bus

    async def execute(
        self, source_buckets: list[str], dest_bucket: str
    ) -> list[STSJobResult]:
        results: list[STSJobResult] = []

        for source_bucket in source_buckets:
            job_id = await self._transfer_port.create_job(source_bucket, dest_bucket)

            status_info = await self._transfer_port.get_job_status(job_id)
            status = status_info.get("status", "pending")

            result = STSJobResult(
                job_id=job_id,
                source_bucket=source_bucket,
                dest_bucket=dest_bucket,
                status=status,
            )
            results.append(result)

            await self._event_bus.publish(
                TransferJobCreated(
                    aggregate_id=job_id,
                    job_id=job_id,
                    source_bucket=source_bucket,
                    dest_bucket=dest_bucket,
                )
            )

        return results
