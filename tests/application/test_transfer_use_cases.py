"""Application-layer tests for Transfer module use cases."""

from __future__ import annotations

import pytest

from athenaforge.application.commands.transfer.plan_delta_compaction import (
    PlanDeltaCompactionUseCase,
)
from athenaforge.application.commands.transfer.model_egress_cost import (
    ModelEgressCostUseCase,
)
from athenaforge.application.commands.transfer.create_sts_jobs import (
    CreateSTSJobsUseCase,
)
from athenaforge.application.commands.transfer.run_dvt_validation import (
    RunDVTValidationUseCase,
)
from athenaforge.application.commands.transfer.control_streaming_cutover import (
    ControlStreamingCutoverUseCase,
)
from athenaforge.domain.events.event_base import DomainEvent
from athenaforge.domain.events.transfer_events import (
    DeltaCompactionStarted,
    DVTValidationCompleted,
    StreamingCutoverInitiated,
    TransferJobCreated,
)
from athenaforge.domain.services.cost_calculator import EgressCostCalculator
from athenaforge.domain.services.delta_log_health_service import DeltaLogHealthService


# ── Stub ports ───────────────────────────────────────────────────────────────


class StubEventBus:
    """Collects all published events."""

    def __init__(self) -> None:
        self.events: list[DomainEvent] = []

    async def publish(self, event: DomainEvent) -> None:
        self.events.append(event)

    def subscribe(self, event_type: type, handler: object) -> None:
        pass


class StubCloudStoragePort:
    """In-memory cloud storage stub."""

    def __init__(
        self,
        objects: dict[str, bytes] | None = None,
        sizes: dict[str, int] | None = None,
    ) -> None:
        self._objects = objects or {}
        self._sizes = sizes or {}

    async def list_objects(self, bucket: str, prefix: str = "") -> list[str]:
        return [k for k in self._objects if k.startswith(prefix)]

    async def read_object(self, bucket: str, key: str) -> bytes:
        return self._objects.get(key, b"")

    async def write_object(self, bucket: str, key: str, data: bytes) -> None:
        self._objects[key] = data

    async def get_object_size(self, bucket: str, key: str) -> int:
        return self._sizes.get(key, len(self._objects.get(key, b"")))


class StubStorageTransferPort:
    """In-memory transfer job stub."""

    def __init__(self) -> None:
        self.created_jobs: list[dict] = []
        self._counter = 0

    async def create_job(
        self,
        source_bucket: str,
        dest_bucket: str,
        include_prefixes: list[str] | None = None,
    ) -> str:
        self._counter += 1
        job_id = f"sts-job-{self._counter}"
        self.created_jobs.append(
            {"job_id": job_id, "source": source_bucket, "dest": dest_bucket}
        )
        return job_id

    async def get_job_status(self, job_id: str) -> dict:
        return {"job_id": job_id, "status": "IN_PROGRESS"}

    async def list_jobs(self) -> list[dict]:
        return self.created_jobs


class StubDVTPort:
    """In-memory DVT stub with configurable results."""

    def __init__(self, results: dict[str, dict] | None = None) -> None:
        self._results = results or {}

    async def validate_row_count(
        self, source_table: str, target_table: str
    ) -> dict:
        key = f"{source_table}:{target_table}"
        return self._results.get(
            key,
            {"status": "pass", "source_count": 100, "target_count": 100},
        )

    async def validate_column_aggregates(
        self, source_table: str, target_table: str, columns: list[str] | None = None
    ) -> dict:
        return {"status": "pass", "mismatches": []}

    async def validate_row_hash(
        self,
        source_table: str,
        target_table: str,
        primary_keys: list[str] | None = None,
    ) -> dict:
        return {"status": "pass", "mismatches": 0}


# ── PlanDeltaCompactionUseCase ───────────────────────────────────────────────


async def test_plan_delta_compaction_returns_plans_for_multiple_tables():
    """Returns one compaction plan per table prefix."""
    # Create storage where each table has a small delta log (< 7 MB → HEALTHY)
    _BYTES_PER_MB = 1_048_576
    objects = {
        "table_a/_delta_log/00001.json": b"x" * 100,
        "table_b/_delta_log/00001.json": b"x" * 100,
    }
    sizes = {
        "table_a/_delta_log/00001.json": 1 * _BYTES_PER_MB,
        "table_b/_delta_log/00001.json": 2 * _BYTES_PER_MB,
    }
    storage = StubCloudStoragePort(objects=objects, sizes=sizes)
    bus = StubEventBus()
    health_svc = DeltaLogHealthService()

    uc = PlanDeltaCompactionUseCase(health_svc, storage, bus)
    plans = await uc.execute("my-bucket", ["table_a", "table_b"])

    assert len(plans) == 2
    assert plans[0].table_name == "table_a"
    assert plans[1].table_name == "table_b"


async def test_plan_delta_compaction_higher_reduction_for_unhealthy_tables():
    """Unhealthy (large log) tables should have a higher estimated reduction."""
    _BYTES_PER_MB = 1_048_576
    # table_healthy: 1 MB (HEALTHY → 0% reduction)
    # table_warning: 8 MB (WARNING → 40% reduction)
    # table_critical: 9.5 MB (CRITICAL → 60% reduction)
    # table_blocked: 11 MB (BLOCKED → 80% reduction)
    objects = {
        "table_healthy/_delta_log/00001.json": b"x",
        "table_warning/_delta_log/00001.json": b"x",
        "table_critical/_delta_log/00001.json": b"x",
        "table_blocked/_delta_log/00001.json": b"x",
    }
    sizes = {
        "table_healthy/_delta_log/00001.json": 1 * _BYTES_PER_MB,
        "table_warning/_delta_log/00001.json": 8 * _BYTES_PER_MB,
        "table_critical/_delta_log/00001.json": int(9.5 * _BYTES_PER_MB),
        "table_blocked/_delta_log/00001.json": 11 * _BYTES_PER_MB,
    }
    storage = StubCloudStoragePort(objects=objects, sizes=sizes)
    bus = StubEventBus()
    health_svc = DeltaLogHealthService()

    uc = PlanDeltaCompactionUseCase(health_svc, storage, bus)
    plans = await uc.execute(
        "bucket",
        ["table_healthy", "table_warning", "table_critical", "table_blocked"],
    )

    assert plans[0].estimated_reduction_pct == 0.0
    assert plans[1].estimated_reduction_pct == 40.0
    assert plans[2].estimated_reduction_pct == 60.0
    assert plans[3].estimated_reduction_pct == 80.0


async def test_plan_delta_compaction_publishes_events():
    """A DeltaCompactionStarted event is published for each table."""
    _BYTES_PER_MB = 1_048_576
    objects = {
        "tbl/_delta_log/00001.json": b"x",
        "tbl2/_delta_log/00001.json": b"x",
    }
    sizes = {
        "tbl/_delta_log/00001.json": 1 * _BYTES_PER_MB,
        "tbl2/_delta_log/00001.json": 1 * _BYTES_PER_MB,
    }
    storage = StubCloudStoragePort(objects=objects, sizes=sizes)
    bus = StubEventBus()
    health_svc = DeltaLogHealthService()

    uc = PlanDeltaCompactionUseCase(health_svc, storage, bus)
    await uc.execute("bucket", ["tbl", "tbl2"])

    compaction_events = [
        e for e in bus.events if isinstance(e, DeltaCompactionStarted)
    ]
    assert len(compaction_events) == 2
    assert compaction_events[0].table_name == "tbl"
    assert compaction_events[1].table_name == "tbl2"


async def test_plan_delta_compaction_handles_empty_prefix_list():
    """An empty prefix list should produce no plans and no events."""
    storage = StubCloudStoragePort()
    bus = StubEventBus()
    health_svc = DeltaLogHealthService()

    uc = PlanDeltaCompactionUseCase(health_svc, storage, bus)
    plans = await uc.execute("bucket", [])

    assert plans == []
    assert bus.events == []


# ── ModelEgressCostUseCase ───────────────────────────────────────────────────


async def test_model_egress_cost_calculates_base_cost_for_1tb():
    """Base egress cost for 1 TB should be a positive USD amount."""
    calculator = EgressCostCalculator()
    bus = StubEventBus()

    uc = ModelEgressCostUseCase(calculator, bus)
    one_tb = 1_099_511_627_776  # 1 TiB in bytes
    report = await uc.execute(one_tb)

    assert report.total_size_bytes == one_tb
    assert report.scenario_base_usd > 0
    assert report.credit_percentage == 0.0


async def test_model_egress_cost_applies_credit_percentage():
    """Credits should reduce the with-credits scenario below the base cost."""
    calculator = EgressCostCalculator()
    bus = StubEventBus()

    uc = ModelEgressCostUseCase(calculator, bus)
    one_tb = 1_099_511_627_776
    report = await uc.execute(one_tb, credit_percentage=10.0)

    assert report.credit_percentage == 10.0
    assert report.scenario_with_credits_usd < report.scenario_base_usd


async def test_model_egress_cost_optimized_includes_compression():
    """The optimized scenario should be less than the base cost due to compression."""
    calculator = EgressCostCalculator()
    bus = StubEventBus()

    uc = ModelEgressCostUseCase(calculator, bus)
    one_tb = 1_099_511_627_776
    report = await uc.execute(one_tb, credit_percentage=0.0)

    # Optimized uses 70% of the data (30% compression), so the cost should
    # be strictly less than the base (no-credit) scenario
    assert report.scenario_optimized_usd < report.scenario_base_usd


async def test_model_egress_cost_zero_bytes_returns_zero():
    """Zero bytes should produce zero for every cost scenario."""
    calculator = EgressCostCalculator()
    bus = StubEventBus()

    uc = ModelEgressCostUseCase(calculator, bus)
    report = await uc.execute(0)

    assert report.total_size_bytes == 0
    assert report.scenario_base_usd == 0.0
    assert report.scenario_with_credits_usd == 0.0
    assert report.scenario_optimized_usd == 0.0


# ── CreateSTSJobsUseCase ────────────────────────────────────────────────────


async def test_create_sts_jobs_creates_job_per_source_bucket():
    """One STS job should be created for each source bucket."""
    transfer = StubStorageTransferPort()
    bus = StubEventBus()

    uc = CreateSTSJobsUseCase(transfer, bus)
    results = await uc.execute(["bucket-a", "bucket-b", "bucket-c"], "dest-bucket")

    assert len(results) == 3
    assert len(transfer.created_jobs) == 3


async def test_create_sts_jobs_returns_correct_ids_and_statuses():
    """Each result should contain the job ID returned by the port and the correct status."""
    transfer = StubStorageTransferPort()
    bus = StubEventBus()

    uc = CreateSTSJobsUseCase(transfer, bus)
    results = await uc.execute(["src-1", "src-2"], "dest")

    assert results[0].job_id == "sts-job-1"
    assert results[1].job_id == "sts-job-2"
    assert results[0].source_bucket == "src-1"
    assert results[1].source_bucket == "src-2"
    assert results[0].dest_bucket == "dest"
    assert results[1].dest_bucket == "dest"
    # StubStorageTransferPort.get_job_status always returns IN_PROGRESS
    assert results[0].status == "IN_PROGRESS"
    assert results[1].status == "IN_PROGRESS"


async def test_create_sts_jobs_publishes_transfer_job_created_events():
    """A TransferJobCreated event should be published for each job."""
    transfer = StubStorageTransferPort()
    bus = StubEventBus()

    uc = CreateSTSJobsUseCase(transfer, bus)
    await uc.execute(["src-a", "src-b"], "dest")

    transfer_events = [
        e for e in bus.events if isinstance(e, TransferJobCreated)
    ]
    assert len(transfer_events) == 2
    assert transfer_events[0].source_bucket == "src-a"
    assert transfer_events[1].source_bucket == "src-b"
    assert transfer_events[0].dest_bucket == "dest"


async def test_create_sts_jobs_handles_single_bucket():
    """A single source bucket should produce exactly one job and one event."""
    transfer = StubStorageTransferPort()
    bus = StubEventBus()

    uc = CreateSTSJobsUseCase(transfer, bus)
    results = await uc.execute(["only-bucket"], "dest")

    assert len(results) == 1
    assert results[0].source_bucket == "only-bucket"
    assert len(bus.events) == 1


# ── RunDVTValidationUseCase ──────────────────────────────────────────────────


async def test_dvt_tier1_runs_all_three_validation_types():
    """Tier 1 should run row_count, column_aggregates, and row_hash."""
    dvt = StubDVTPort()
    bus = StubEventBus()

    uc = RunDVTValidationUseCase(dvt, bus)
    report = await uc.execute("tier1", [("src.t1", "tgt.t1")])

    assert report.tier == "tier1"
    assert report.tables_validated == 1
    assert report.tables_passed == 1
    assert report.tables_failed == 0
    # Verify all three checks were passed
    detail = report.details[0]
    assert "row_count" in detail["checks_passed"]
    assert "column_aggregates" in detail["checks_passed"]
    assert "row_hash" in detail["checks_passed"]


async def test_dvt_tier2_runs_row_count_and_column_aggregates():
    """Tier 2 should run row_count and column_aggregates but NOT row_hash."""
    dvt = StubDVTPort()
    bus = StubEventBus()

    uc = RunDVTValidationUseCase(dvt, bus)
    report = await uc.execute("tier2", [("src.t1", "tgt.t1")])

    assert report.tables_validated == 1
    detail = report.details[0]
    assert "row_count" in detail["checks_passed"]
    assert "column_aggregates" in detail["checks_passed"]
    assert "row_hash" not in detail["checks_passed"]
    assert "row_hash" not in detail.get("checks_failed", "")


async def test_dvt_tier3_runs_only_row_count():
    """Tier 3 should run only row_count."""
    dvt = StubDVTPort()
    bus = StubEventBus()

    uc = RunDVTValidationUseCase(dvt, bus)
    report = await uc.execute("tier3", [("src.t1", "tgt.t1")])

    assert report.tables_validated == 1
    detail = report.details[0]
    assert "row_count" in detail["checks_passed"]
    assert "column_aggregates" not in detail["checks_passed"]
    assert "row_hash" not in detail["checks_passed"]


async def test_dvt_reports_pass_fail_correctly():
    """Tables that fail validation should be counted as failed."""
    # Make one table fail row_count validation
    dvt = StubDVTPort(
        results={
            "src.bad:tgt.bad": {
                "status": "fail",
                "source_count": 100,
                "target_count": 50,
            }
        }
    )
    bus = StubEventBus()

    uc = RunDVTValidationUseCase(dvt, bus)
    report = await uc.execute(
        "tier3", [("src.good", "tgt.good"), ("src.bad", "tgt.bad")]
    )

    assert report.tables_validated == 2
    assert report.tables_passed == 1
    assert report.tables_failed == 1


async def test_dvt_publishes_validation_completed_event():
    """A DVTValidationCompleted event should be published after validation."""
    dvt = StubDVTPort()
    bus = StubEventBus()

    uc = RunDVTValidationUseCase(dvt, bus)
    await uc.execute("tier1", [("src.t1", "tgt.t1"), ("src.t2", "tgt.t2")])

    dvt_events = [
        e for e in bus.events if isinstance(e, DVTValidationCompleted)
    ]
    assert len(dvt_events) == 1
    assert dvt_events[0].tier == "tier1"
    assert dvt_events[0].tables_validated == 2
    assert dvt_events[0].tables_passed == 2


# ── ControlStreamingCutoverUseCase ───────────────────────────────────────────


async def test_streaming_cutover_returns_correct_fields():
    """The cutover result should contain all the input fields and a valid status."""
    bus = StubEventBus()

    uc = ControlStreamingCutoverUseCase(bus)
    result = await uc.execute(
        job_id="job-1",
        source_topic="topic-src",
        target_topic="topic-tgt",
        current_lag=42,
    )

    assert result.job_id == "job-1"
    assert result.source_topic == "topic-src"
    assert result.target_topic == "topic-tgt"
    assert result.lag_at_cutover == 42
    # After initiate_drain -> switch_target the status should be "switching"
    assert result.status == "switching"


async def test_streaming_cutover_publishes_events():
    """At least one streaming cutover event should be published."""
    bus = StubEventBus()

    uc = ControlStreamingCutoverUseCase(bus)
    await uc.execute(
        job_id="job-2",
        source_topic="src",
        target_topic="tgt",
        current_lag=0,
    )

    cutover_events = [
        e for e in bus.events if isinstance(e, StreamingCutoverInitiated)
    ]
    assert len(cutover_events) >= 1
    assert cutover_events[0].job_id == "job-2"
    assert cutover_events[0].source_topic == "src"
    assert cutover_events[0].target_topic == "tgt"
