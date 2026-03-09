from __future__ import annotations

from athenaforge.application.dtos.dependency_dtos import DependencyScanReport
from athenaforge.domain.events.dependency_events import DependencyScanCompleted
from athenaforge.domain.ports.cloud_storage_port import CloudStoragePort
from athenaforge.domain.ports.event_bus import EventBusPort
from athenaforge.domain.services.dependency_scanner import DependencyScanner
from athenaforge.domain.value_objects.dependency_ref import JobType


class ScanSparkFlinkJobsUseCase:
    """Scan cloud storage for Spark/Flink job files and identify dependencies."""

    def __init__(
        self,
        scanner: DependencyScanner,
        storage_port: CloudStoragePort,
        event_bus: EventBusPort,
    ) -> None:
        self._scanner = scanner
        self._storage_port = storage_port
        self._event_bus = event_bus

    async def execute(
        self, bucket: str, prefixes: list[str]
    ) -> DependencyScanReport:
        spark_jobs = 0
        flink_jobs = 0
        dags = 0
        lambdas = 0
        total_references = 0
        details: list[dict[str, object]] = []

        for prefix in prefixes:
            keys = await self._storage_port.list_objects(bucket, prefix)
            for key in keys:
                raw = await self._storage_port.read_object(bucket, key)
                content = raw.decode("utf-8", errors="replace")
                refs = self._scanner.scan(content, key)

                for ref in refs:
                    total_references += len(ref.references)
                    if ref.job_type == JobType.SPARK:
                        spark_jobs += 1
                    elif ref.job_type == JobType.FLINK:
                        flink_jobs += 1
                    elif ref.job_type == JobType.AIRFLOW_DAG:
                        dags += 1
                    elif ref.job_type == JobType.LAMBDA:
                        lambdas += 1

                    details.append(
                        {
                            "file": ref.source_path,
                            "job_type": ref.job_type.value,
                            "references": list(ref.references),
                        }
                    )

        await self._event_bus.publish(
            DependencyScanCompleted(
                aggregate_id=bucket,
                spark_jobs=spark_jobs,
                flink_jobs=flink_jobs,
                dags=dags,
                lambdas=lambdas,
            )
        )

        return DependencyScanReport(
            spark_jobs=spark_jobs,
            flink_jobs=flink_jobs,
            dags=dags,
            lambdas=lambdas,
            total_references=total_references,
            details=details,
        )
