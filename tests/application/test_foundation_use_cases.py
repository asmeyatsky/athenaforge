"""Application-layer tests for Foundation module use cases."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from athenaforge.application.commands.foundation.bootstrap_dataplex import (
    BootstrapDataplexUseCase,
)
from athenaforge.application.commands.foundation.check_delta_health import (
    CheckDeltaHealthUseCase,
)
from athenaforge.application.commands.foundation.classify_tiers import (
    ClassifyTiersUseCase,
)
from athenaforge.application.commands.foundation.configure_pricing import (
    ConfigurePricingUseCase,
)
from athenaforge.domain.entities.table_inventory import TableEntry, TableInventory
from athenaforge.domain.events.event_base import DomainEvent
from athenaforge.domain.events.foundation_events import (
    DataplexBootstrapCompleted,
    DeltaLogHealthChecked,
    TierClassificationCompleted,
)
from athenaforge.domain.services.cost_calculator import SlotPricingCalculator
from athenaforge.domain.services.delta_log_health_service import DeltaLogHealthService
from athenaforge.domain.services.tier_classification_service import (
    TierClassificationService,
)


# ── Stub ports ───────────────────────────────────────────────────────────────


class StubEventBus:
    """Collects all published events."""

    def __init__(self) -> None:
        self.events: list[DomainEvent] = []

    async def publish(self, event: DomainEvent) -> None:
        self.events.append(event)

    def subscribe(self, event_type: type, handler: object) -> None:
        pass


class StubReadRepositoryPort:
    """In-memory read repository that returns pre-loaded entities by id."""

    def __init__(self, entities: dict[str, object] | None = None) -> None:
        self._store = entities or {}

    async def get_by_id(self, id: str) -> object | None:
        return self._store.get(id)

    async def list_all(self) -> list[object]:
        return list(self._store.values())


class StubTerraformGeneratorPort:
    """Records every template render and file write."""

    def __init__(self) -> None:
        self.rendered: list[tuple[str, dict]] = []
        self.written_files: list[tuple[str, str]] = []

    def render_template(self, template_name: str, context: dict) -> str:
        self.rendered.append((template_name, context))
        return f"# rendered {template_name}"

    def write_file(self, output_path: str, content: str) -> None:
        self.written_files.append((output_path, content))


class StubCloudStoragePort:
    """In-memory cloud storage with configurable object listings and sizes."""

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


# ── Helpers ──────────────────────────────────────────────────────────────────

_BYTES_PER_MB = 1_048_576
_ONE_TB = 1_099_511_627_776


def _recent_date() -> datetime:
    """Return a recent datetime (today) so the table counts as recently queried."""
    return datetime.now(tz=timezone.utc)


def _build_inventory(
    inventory_id: str, tables: list[TableEntry]
) -> TableInventory:
    return TableInventory(inventory_id=inventory_id, tables=tuple(tables))


# ── ClassifyTiersUseCase tests ───────────────────────────────────────────────


async def test_classify_tiers_classifies_all_three_tiers():
    """Tables should be distributed across Tier 1, 2, and 3 based on size and query recency."""
    tier_service = TierClassificationService()
    bus = StubEventBus()

    tables = [
        # Tier 1: small, recently queried
        TableEntry(
            table_name="small_table",
            database="db",
            size_bytes=100_000,
            row_count=10,
            last_queried=_recent_date(),
            partitioned=False,
        ),
        # Tier 2: large (>= 1 TB), recently queried
        TableEntry(
            table_name="large_table",
            database="db",
            size_bytes=_ONE_TB + 1,
            row_count=1_000_000,
            last_queried=_recent_date(),
            partitioned=True,
        ),
        # Tier 3: never queried
        TableEntry(
            table_name="stale_table",
            database="db",
            size_bytes=500,
            row_count=5,
            last_queried=None,
            partitioned=False,
        ),
    ]
    inventory = _build_inventory("inv-001", tables)
    repo = StubReadRepositoryPort({"inv-001": inventory})

    uc = ClassifyTiersUseCase(tier_service, repo, bus)
    result = await uc.execute("inv-001")

    assert result.classifications["small_table"] == "TIER_1"
    assert result.classifications["large_table"] == "TIER_2"
    assert result.classifications["stale_table"] == "TIER_3"


async def test_classify_tiers_publishes_completed_event():
    """A TierClassificationCompleted event must be published."""
    tier_service = TierClassificationService()
    bus = StubEventBus()

    tables = [
        TableEntry(
            table_name="t1",
            database="db",
            size_bytes=100,
            row_count=1,
            last_queried=_recent_date(),
            partitioned=False,
        ),
    ]
    inventory = _build_inventory("inv-002", tables)
    repo = StubReadRepositoryPort({"inv-002": inventory})

    uc = ClassifyTiersUseCase(tier_service, repo, bus)
    await uc.execute("inv-002")

    tier_events = [e for e in bus.events if isinstance(e, TierClassificationCompleted)]
    assert len(tier_events) == 1
    assert tier_events[0].aggregate_id == "inv-002"
    assert tier_events[0].total_tables == 1


async def test_classify_tiers_returns_correct_counts():
    """The returned ClassificationResult must have accurate tier counts."""
    tier_service = TierClassificationService()
    bus = StubEventBus()

    tables = [
        # Two Tier 1 tables
        TableEntry(
            table_name="a",
            database="db",
            size_bytes=1000,
            row_count=1,
            last_queried=_recent_date(),
            partitioned=False,
        ),
        TableEntry(
            table_name="b",
            database="db",
            size_bytes=2000,
            row_count=2,
            last_queried=_recent_date(),
            partitioned=False,
        ),
        # One Tier 3 table
        TableEntry(
            table_name="c",
            database="db",
            size_bytes=500,
            row_count=1,
            last_queried=None,
            partitioned=False,
        ),
    ]
    inventory = _build_inventory("inv-003", tables)
    repo = StubReadRepositoryPort({"inv-003": inventory})

    uc = ClassifyTiersUseCase(tier_service, repo, bus)
    result = await uc.execute("inv-003")

    assert result.total_tables == 3
    assert result.tier1_count == 2
    assert result.tier2_count == 0
    assert result.tier3_count == 1


async def test_classify_tiers_raises_for_missing_inventory():
    """Calling execute with an unknown inventory_id must raise ValueError."""
    tier_service = TierClassificationService()
    bus = StubEventBus()
    repo = StubReadRepositoryPort({})

    uc = ClassifyTiersUseCase(tier_service, repo, bus)

    with pytest.raises(ValueError, match="not found"):
        await uc.execute("nonexistent-id")


# ── ConfigurePricingUseCase tests ────────────────────────────────────────────


async def test_configure_pricing_one_year_commitment():
    """A 1-year commitment should use the $0.04/slot/hr rate."""
    calculator = SlotPricingCalculator()
    tf = StubTerraformGeneratorPort()

    uc = ConfigurePricingUseCase(calculator, tf)
    result = await uc.execute(slots=100, commitment_years=1, output_dir="/out")

    # 100 slots * $0.04/hr * 720 hrs = $2880.00
    assert result.edition == "Enterprise"
    assert result.slots == 100
    assert result.monthly_cost_usd == float(Decimal("100") * Decimal("0.04") * Decimal("720"))


async def test_configure_pricing_three_year_commitment():
    """A 3-year commitment should use the $0.032/slot/hr rate."""
    calculator = SlotPricingCalculator()
    tf = StubTerraformGeneratorPort()

    uc = ConfigurePricingUseCase(calculator, tf)
    result = await uc.execute(slots=200, commitment_years=3, output_dir="/out")

    # 200 slots * $0.032/hr * 720 hrs = $4608.00
    expected = float(Decimal("200") * Decimal("0.032") * Decimal("720"))
    assert result.monthly_cost_usd == expected
    assert result.slots == 200


async def test_configure_pricing_renders_reservation_template():
    """The use case must render the reservation.tf template via the Terraform port."""
    calculator = SlotPricingCalculator()
    tf = StubTerraformGeneratorPort()

    uc = ConfigurePricingUseCase(calculator, tf)
    await uc.execute(slots=100, commitment_years=1, output_dir="/terraform")

    assert len(tf.rendered) == 1
    template_name, context = tf.rendered[0]
    assert template_name == "reservation.tf"
    assert context["edition"] == "Enterprise"
    assert context["slots"] == 100
    assert context["commitment_years"] == 1


async def test_configure_pricing_returns_terraform_file_path():
    """The result must include the correct terraform file path."""
    calculator = SlotPricingCalculator()
    tf = StubTerraformGeneratorPort()

    uc = ConfigurePricingUseCase(calculator, tf)
    result = await uc.execute(slots=100, commitment_years=1, output_dir="/infra")

    assert result.terraform_file == "/infra/reservation.tf"
    assert len(tf.written_files) == 1
    written_path, _ = tf.written_files[0]
    assert written_path == "/infra/reservation.tf"


# ── CheckDeltaHealthUseCase tests ────────────────────────────────────────────


async def test_check_delta_health_reports_healthy_tables():
    """Tables with small Delta logs should be reported as HEALTHY."""
    health_service = DeltaLogHealthService()
    bus = StubEventBus()

    # 1 MB of delta log data (well under threshold)
    log_size = 1 * _BYTES_PER_MB
    storage = StubCloudStoragePort(
        objects={
            "tables/orders/_delta_log/00000.json": b"x" * log_size,
        },
        sizes={
            "tables/orders/_delta_log/00000.json": log_size,
        },
    )

    uc = CheckDeltaHealthUseCase(health_service, storage, bus)
    results = await uc.execute("my-bucket", "tables/")

    assert len(results) == 1
    assert results[0].table_name == "orders"
    assert results[0].status == "HEALTHY"
    assert results[0].recommendation == "No action needed"


async def test_check_delta_health_reports_warning_critical_blocked():
    """Tables at different log sizes should receive WARNING, CRITICAL, and BLOCKED statuses."""
    health_service = DeltaLogHealthService()
    bus = StubEventBus()

    warning_size = int(7.5 * _BYTES_PER_MB)
    critical_size = int(9.5 * _BYTES_PER_MB)
    blocked_size = int(12.0 * _BYTES_PER_MB)

    storage = StubCloudStoragePort(
        objects={
            "data/warn_tbl/_delta_log/00000.json": b"w" * warning_size,
            "data/crit_tbl/_delta_log/00000.json": b"c" * critical_size,
            "data/block_tbl/_delta_log/00000.json": b"b" * blocked_size,
        },
        sizes={
            "data/warn_tbl/_delta_log/00000.json": warning_size,
            "data/crit_tbl/_delta_log/00000.json": critical_size,
            "data/block_tbl/_delta_log/00000.json": blocked_size,
        },
    )

    uc = CheckDeltaHealthUseCase(health_service, storage, bus)
    results = await uc.execute("bucket", "data/")

    status_map = {r.table_name: r.status for r in results}
    assert status_map["warn_tbl"] == "WARNING"
    assert status_map["crit_tbl"] == "CRITICAL"
    assert status_map["block_tbl"] == "BLOCKED"


async def test_check_delta_health_publishes_events():
    """A DeltaLogHealthChecked event must be published for each table."""
    health_service = DeltaLogHealthService()
    bus = StubEventBus()

    log_size = 2 * _BYTES_PER_MB
    storage = StubCloudStoragePort(
        objects={
            "pfx/t1/_delta_log/00000.json": b"a" * log_size,
            "pfx/t2/_delta_log/00000.json": b"b" * log_size,
        },
        sizes={
            "pfx/t1/_delta_log/00000.json": log_size,
            "pfx/t2/_delta_log/00000.json": log_size,
        },
    )

    uc = CheckDeltaHealthUseCase(health_service, storage, bus)
    await uc.execute("bucket", "pfx/")

    health_events = [e for e in bus.events if isinstance(e, DeltaLogHealthChecked)]
    assert len(health_events) == 2

    event_tables = {e.table_name for e in health_events}
    assert event_tables == {"t1", "t2"}


async def test_check_delta_health_handles_empty_bucket():
    """An empty bucket (no delta log objects) should return an empty result list."""
    health_service = DeltaLogHealthService()
    bus = StubEventBus()
    storage = StubCloudStoragePort(objects={}, sizes={})

    uc = CheckDeltaHealthUseCase(health_service, storage, bus)
    results = await uc.execute("empty-bucket", "prefix/")

    assert results == []
    assert len(bus.events) == 0


# ── BootstrapDataplexUseCase tests ───────────────────────────────────────────


async def test_bootstrap_dataplex_returns_result_with_lake_name():
    """The result should contain the lake name and zone count."""
    bus = StubEventBus()

    uc = BootstrapDataplexUseCase(bus)
    result = await uc.execute("analytics-lake", ["raw", "curated", "sandbox"])

    assert result.lakes_created == ["analytics-lake"]
    assert result.zones_per_lake == 3
    assert result.dlp_scans_scheduled == 0
    assert result.policy_tags_applied == 0


async def test_bootstrap_dataplex_publishes_completed_event():
    """A DataplexBootstrapCompleted event must be published with the lake name."""
    bus = StubEventBus()

    uc = BootstrapDataplexUseCase(bus)
    await uc.execute("my-lake", ["raw", "curated"])

    bootstrap_events = [
        e for e in bus.events if isinstance(e, DataplexBootstrapCompleted)
    ]
    assert len(bootstrap_events) == 1
    assert bootstrap_events[0].aggregate_id == "my-lake"
    assert bootstrap_events[0].lake_name == "my-lake"
    assert bootstrap_events[0].zones_created == 2
