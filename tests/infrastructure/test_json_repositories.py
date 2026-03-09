from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from athenaforge.domain.entities.streaming_job import StreamingJob
from athenaforge.domain.entities.table_inventory import TableEntry, TableInventory
from athenaforge.domain.entities.transfer_job import TransferJob
from athenaforge.domain.entities.translation_batch import (
    TranslationBatch,
    TranslationFile,
)
from athenaforge.domain.entities.wave import Wave
from athenaforge.domain.value_objects.tier import Tier, TierClassification
from athenaforge.domain.value_objects.wave import ParallelRunMode, WaveStatus
from athenaforge.infrastructure.repositories import (
    StreamingJobRepository,
    TableInventoryRepository,
    TransferJobRepository,
    TranslationBatchRepository,
    WaveRepository,
)


# ---------------------------------------------------------------------------
# Sample entity builders
# ---------------------------------------------------------------------------

def _make_table_inventory(inventory_id: str = "inv-001") -> TableInventory:
    tables = (
        TableEntry(
            table_name="orders",
            database="analytics",
            size_bytes=500_000_000,
            row_count=1_000_000,
            last_queried=datetime(2025, 6, 1, 12, 0, 0),
            partitioned=True,
            format="PARQUET",
            has_maps=False,
        ),
        TableEntry(
            table_name="users",
            database="analytics",
            size_bytes=100_000_000,
            row_count=50_000,
            last_queried=None,
            partitioned=False,
            format="ORC",
            has_maps=True,
        ),
    )
    classifications = {
        "orders": TierClassification(
            table_name="orders",
            tier=Tier.TIER_1,
            reason="Active and small",
            size_bytes=500_000_000,
            last_queried_days_ago=30,
        ),
        "users": TierClassification(
            table_name="users",
            tier=Tier.TIER_3,
            reason="Never queried",
            size_bytes=100_000_000,
            last_queried_days_ago=None,
        ),
    }
    return TableInventory(
        inventory_id=inventory_id,
        tables=tables,
        classifications=classifications,
    )


def _make_translation_batch(batch_id: str = "batch-001") -> TranslationBatch:
    files = (
        TranslationFile(
            file_path="/sql/query1.sql",
            status="translated",
            error=None,
            translated_path="/out/query1.sql",
        ),
        TranslationFile(
            file_path="/sql/query2.sql",
            status="pending",
            error=None,
            translated_path=None,
        ),
    )
    return TranslationBatch(
        batch_id=batch_id,
        files=files,
        status="in_progress",
    )


def _make_wave(wave_id: str = "wave-001") -> Wave:
    return Wave(
        wave_id=wave_id,
        name="Wave Alpha",
        lob="finance",
        tables=("orders", "payments", "invoices"),
        status=WaveStatus.SHADOW_RUNNING,
        mode=ParallelRunMode.SHADOW,
        dvt_results={"orders": True, "payments": False},
    )


def _make_transfer_job(job_id: str = "xfer-001") -> TransferJob:
    return TransferJob(
        job_id=job_id,
        source_bucket="s3-source-bucket",
        destination_bucket="gs-dest-bucket",
        total_bytes=10_000_000_000,
        bytes_transferred=3_000_000_000,
        status="in_progress",
        created_at=datetime(2025, 7, 15, 8, 30, 0),
    )


def _make_streaming_job(job_id: str = "stream-001") -> StreamingJob:
    return StreamingJob(
        job_id=job_id,
        source_topic="events-raw",
        target_topic="events-bq",
        consumer_group="analytics-consumers",
        status="active",
        lag_threshold=500,
        current_lag=42,
    )


# ---------------------------------------------------------------------------
# TableInventoryRepository
# ---------------------------------------------------------------------------


class TestTableInventoryRepository:
    """Tests for JSON-backed TableInventoryRepository."""

    @pytest.fixture()
    def repo(self, tmp_path: Path) -> TableInventoryRepository:
        return TableInventoryRepository(data_dir=str(tmp_path))

    async def test_save_and_get_by_id_roundtrip(
        self, repo: TableInventoryRepository
    ) -> None:
        entity = _make_table_inventory("inv-rt")
        await repo.save(entity)

        loaded = await repo.get_by_id("inv-rt")

        assert loaded is not None
        assert loaded.inventory_id == "inv-rt"
        assert len(loaded.tables) == 2
        assert loaded.tables[0].table_name == "orders"
        assert loaded.tables[0].size_bytes == 500_000_000
        assert loaded.tables[0].last_queried == datetime(2025, 6, 1, 12, 0, 0)
        assert loaded.tables[1].last_queried is None
        assert loaded.tables[1].has_maps is True
        assert "orders" in loaded.classifications
        assert loaded.classifications["orders"].tier == Tier.TIER_1
        assert loaded.classifications["users"].tier == Tier.TIER_3
        assert loaded.classifications["users"].last_queried_days_ago is None

    async def test_list_all(self, repo: TableInventoryRepository) -> None:
        await repo.save(_make_table_inventory("inv-a"))
        await repo.save(_make_table_inventory("inv-b"))

        all_items = await repo.list_all()

        assert len(all_items) == 2
        ids = {item.inventory_id for item in all_items}
        assert ids == {"inv-a", "inv-b"}

    async def test_delete(self, repo: TableInventoryRepository) -> None:
        await repo.save(_make_table_inventory("inv-del"))
        assert await repo.get_by_id("inv-del") is not None

        await repo.delete("inv-del")

        assert await repo.get_by_id("inv-del") is None

    async def test_get_by_id_nonexistent_returns_none(
        self, repo: TableInventoryRepository
    ) -> None:
        result = await repo.get_by_id("does-not-exist")
        assert result is None


# ---------------------------------------------------------------------------
# TranslationBatchRepository
# ---------------------------------------------------------------------------


class TestTranslationBatchRepository:
    """Tests for JSON-backed TranslationBatchRepository."""

    @pytest.fixture()
    def repo(self, tmp_path: Path) -> TranslationBatchRepository:
        return TranslationBatchRepository(data_dir=str(tmp_path))

    async def test_save_and_get_by_id_roundtrip(
        self, repo: TranslationBatchRepository
    ) -> None:
        entity = _make_translation_batch("batch-rt")
        await repo.save(entity)

        loaded = await repo.get_by_id("batch-rt")

        assert loaded is not None
        assert loaded.batch_id == "batch-rt"
        assert loaded.status == "in_progress"
        assert len(loaded.files) == 2
        assert loaded.files[0].file_path == "/sql/query1.sql"
        assert loaded.files[0].status == "translated"
        assert loaded.files[0].translated_path == "/out/query1.sql"
        assert loaded.files[1].status == "pending"
        assert loaded.files[1].error is None

    async def test_get_by_id_nonexistent_returns_none(
        self, repo: TranslationBatchRepository
    ) -> None:
        result = await repo.get_by_id("no-such-batch")
        assert result is None


# ---------------------------------------------------------------------------
# WaveRepository
# ---------------------------------------------------------------------------


class TestWaveRepository:
    """Tests for JSON-backed WaveRepository with enum serialization."""

    @pytest.fixture()
    def repo(self, tmp_path: Path) -> WaveRepository:
        return WaveRepository(data_dir=str(tmp_path))

    async def test_save_and_get_by_id_with_enum_serialization(
        self, repo: WaveRepository
    ) -> None:
        entity = _make_wave("wave-rt")
        await repo.save(entity)

        loaded = await repo.get_by_id("wave-rt")

        assert loaded is not None
        assert loaded.wave_id == "wave-rt"
        assert loaded.name == "Wave Alpha"
        assert loaded.lob == "finance"
        assert loaded.tables == ("orders", "payments", "invoices")
        assert loaded.status == WaveStatus.SHADOW_RUNNING
        assert loaded.mode == ParallelRunMode.SHADOW
        assert loaded.dvt_results == {"orders": True, "payments": False}

    async def test_get_by_id_nonexistent_returns_none(
        self, repo: WaveRepository
    ) -> None:
        result = await repo.get_by_id("no-such-wave")
        assert result is None


# ---------------------------------------------------------------------------
# TransferJobRepository
# ---------------------------------------------------------------------------


class TestTransferJobRepository:
    """Tests for JSON-backed TransferJobRepository."""

    @pytest.fixture()
    def repo(self, tmp_path: Path) -> TransferJobRepository:
        return TransferJobRepository(data_dir=str(tmp_path))

    async def test_save_and_get_by_id_roundtrip(
        self, repo: TransferJobRepository
    ) -> None:
        entity = _make_transfer_job("xfer-rt")
        await repo.save(entity)

        loaded = await repo.get_by_id("xfer-rt")

        assert loaded is not None
        assert loaded.job_id == "xfer-rt"
        assert loaded.source_bucket == "s3-source-bucket"
        assert loaded.destination_bucket == "gs-dest-bucket"
        assert loaded.total_bytes == 10_000_000_000
        assert loaded.bytes_transferred == 3_000_000_000
        assert loaded.status == "in_progress"
        assert loaded.created_at == datetime(2025, 7, 15, 8, 30, 0)

    async def test_get_by_id_nonexistent_returns_none(
        self, repo: TransferJobRepository
    ) -> None:
        result = await repo.get_by_id("no-such-xfer")
        assert result is None


# ---------------------------------------------------------------------------
# StreamingJobRepository
# ---------------------------------------------------------------------------


class TestStreamingJobRepository:
    """Tests for JSON-backed StreamingJobRepository."""

    @pytest.fixture()
    def repo(self, tmp_path: Path) -> StreamingJobRepository:
        return StreamingJobRepository(data_dir=str(tmp_path))

    async def test_save_and_get_by_id_roundtrip(
        self, repo: StreamingJobRepository
    ) -> None:
        entity = _make_streaming_job("stream-rt")
        await repo.save(entity)

        loaded = await repo.get_by_id("stream-rt")

        assert loaded is not None
        assert loaded.job_id == "stream-rt"
        assert loaded.source_topic == "events-raw"
        assert loaded.target_topic == "events-bq"
        assert loaded.consumer_group == "analytics-consumers"
        assert loaded.status == "active"
        assert loaded.lag_threshold == 500
        assert loaded.current_lag == 42

    async def test_get_by_id_nonexistent_returns_none(
        self, repo: StreamingJobRepository
    ) -> None:
        result = await repo.get_by_id("no-such-stream")
        assert result is None
