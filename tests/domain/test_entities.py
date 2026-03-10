from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone

import pytest

from athenaforge.domain.entities.migration_project import MigrationProject
from athenaforge.domain.entities.streaming_job import StreamingJob
from athenaforge.domain.entities.table_inventory import TableEntry, TableInventory
from athenaforge.domain.entities.transfer_job import TransferJob
from athenaforge.domain.entities.translation_batch import TranslationBatch, TranslationFile
from athenaforge.domain.entities.wave import Wave
from athenaforge.domain.services.tier_classification_service import TierClassificationService
from athenaforge.domain.value_objects.tier import Tier
from athenaforge.domain.value_objects.wave import ParallelRunMode, WaveStatus

_NOW = datetime.now(tz=timezone.utc)

# ---------------------------------------------------------------------------
# MigrationProject
# ---------------------------------------------------------------------------


class TestMigrationProject:
    def _make_project(self) -> MigrationProject:
        return MigrationProject(
            project_id="proj-1",
            name="Test Migration",
            gcp_project_id="gcp-test",
            aws_region="ap-south-1",
        )

    def test_frozen(self) -> None:
        project = self._make_project()
        with pytest.raises(FrozenInstanceError):
            project.name = "changed"  # type: ignore[misc]

    def test_add_lob_returns_new_instance(self) -> None:
        project = self._make_project()
        updated = project.add_lob("finance")
        assert "finance" in updated.lobs
        assert "finance" not in project.lobs
        assert updated is not project

    def test_start_scaffolding_changes_status(self) -> None:
        project = self._make_project()
        assert project.status == "initialized"
        scaffolding = project.start_scaffolding()
        assert scaffolding.status == "scaffolding"
        assert project.status == "initialized"

    def test_collect_events_returns_accumulated(self) -> None:
        project = self._make_project()
        updated = project.add_lob("marketing")
        events = updated.collect_events()
        assert len(events) == 1
        assert events[0].event_type == "ScaffoldGenerated"
        # events are immutable — same call returns same events
        assert updated.collect_events() == events

    def test_clear_events_returns_clean_instance(self) -> None:
        project = self._make_project()
        updated = project.add_lob("marketing")
        assert len(updated.collect_events()) == 1
        cleared = updated.clear_events()
        assert len(cleared.collect_events()) == 0
        # original still has events
        assert len(updated.collect_events()) == 1


# ---------------------------------------------------------------------------
# TableInventory
# ---------------------------------------------------------------------------


class TestTableInventory:
    def _make_entry(
        self,
        name: str,
        size_bytes: int = 100 * 1024**3,
        days_ago: int | None = 10,
        has_maps: bool = False,
    ) -> TableEntry:
        last_queried = _NOW - timedelta(days=days_ago) if days_ago is not None else None
        return TableEntry(
            table_name=name,
            database="analytics",
            size_bytes=size_bytes,
            row_count=1_000,
            last_queried=last_queried,
            partitioned=True,
            has_maps=has_maps,
        )

    def _make_inventory(self) -> TableInventory:
        tables = (
            self._make_entry("small_active", size_bytes=50 * 1024**3, days_ago=5),
            self._make_entry("large_active", size_bytes=2 * 1024**4, days_ago=5),
            self._make_entry("inactive", days_ago=120),
            self._make_entry("with_maps", has_maps=True),
        )
        return TableInventory(inventory_id="inv-1", tables=tables)

    def test_get_by_tier(self) -> None:
        inv = self._make_inventory()
        svc = TierClassificationService()
        classified = inv.classify_all(svc)

        tier1 = classified.get_by_tier(Tier.TIER_1)
        tier2 = classified.get_by_tier(Tier.TIER_2)
        tier3 = classified.get_by_tier(Tier.TIER_3)

        tier1_names = {t.table_name for t in tier1}
        tier2_names = {t.table_name for t in tier2}
        tier3_names = {t.table_name for t in tier3}

        assert "small_active" in tier1_names
        assert "with_maps" in tier1_names
        assert "large_active" in tier2_names
        assert "inactive" in tier3_names

    def test_get_tables_with_maps(self) -> None:
        inv = self._make_inventory()
        map_tables = inv.get_tables_with_maps()
        assert len(map_tables) == 1
        assert map_tables[0].table_name == "with_maps"

    def test_classify_all(self) -> None:
        inv = self._make_inventory()
        svc = TierClassificationService()
        classified = inv.classify_all(svc)

        assert len(classified.classifications) == len(inv.tables)
        events = classified.collect_events()
        assert len(events) == 1
        assert events[0].event_type == "TierClassificationCompleted"


# ---------------------------------------------------------------------------
# TranslationBatch
# ---------------------------------------------------------------------------


class TestTranslationBatch:
    def _make_batch(self) -> TranslationBatch:
        files = (
            TranslationFile(file_path="query1.sql"),
            TranslationFile(file_path="query2.sql"),
            TranslationFile(file_path="query3.sql"),
            TranslationFile(file_path="query4.sql"),
        )
        return TranslationBatch(batch_id="batch-1", files=files)

    def test_mark_file_translated(self) -> None:
        batch = self._make_batch()
        updated = batch.mark_file_translated("query1.sql", "/out/query1.sql")
        translated = [f for f in updated.files if f.file_path == "query1.sql"][0]
        assert translated.status == "translated"
        assert translated.translated_path == "/out/query1.sql"

    def test_mark_file_failed(self) -> None:
        batch = self._make_batch()
        updated = batch.mark_file_failed("query2.sql", "syntax error")
        failed = [f for f in updated.files if f.file_path == "query2.sql"][0]
        assert failed.status == "failed"
        assert failed.error == "syntax error"

    def test_completion_percentage_partial(self) -> None:
        batch = self._make_batch()
        batch = batch.mark_file_translated("query1.sql", "/out/query1.sql")
        assert batch.completion_percentage == 25.0

    def test_completion_percentage_full(self) -> None:
        batch = self._make_batch()
        batch = batch.mark_file_translated("query1.sql", "/out/query1.sql")
        batch = batch.mark_file_translated("query2.sql", "/out/query2.sql")
        batch = batch.mark_file_failed("query3.sql", "error")
        batch = batch.mark_file_translated("query4.sql", "/out/query4.sql")
        assert batch.completion_percentage == 100.0
        assert batch.status == "completed"

    def test_completion_percentage_empty_batch(self) -> None:
        batch = TranslationBatch(batch_id="empty", files=())
        assert batch.completion_percentage == 100.0

    def test_collect_events_after_completion(self) -> None:
        files = (
            TranslationFile(file_path="a.sql"),
            TranslationFile(file_path="b.sql"),
        )
        batch = TranslationBatch(batch_id="batch-ev", files=files)
        batch = batch.mark_file_translated("a.sql", "/out/a.sql")
        batch = batch.mark_file_translated("b.sql", "/out/b.sql")
        events = batch.collect_events()
        assert len(events) == 1
        assert events[0].event_type == "TranslationBatchCompleted"


# ---------------------------------------------------------------------------
# TransferJob
# ---------------------------------------------------------------------------


class TestTransferJob:
    def _make_job(self) -> TransferJob:
        return TransferJob(
            job_id="xfer-1",
            source_bucket="s3://source",
            destination_bucket="gs://dest",
            total_bytes=1_000_000,
            bytes_transferred=250_000,
        )

    def test_progress_percentage(self) -> None:
        job = self._make_job()
        assert job.progress_percentage == 25.0

    def test_progress_zero_total(self) -> None:
        job = TransferJob(
            job_id="empty",
            source_bucket="s3://a",
            destination_bucket="gs://b",
            total_bytes=0,
        )
        assert job.progress_percentage == 100.0

    def test_mark_completed(self) -> None:
        job = self._make_job()
        completed = job.mark_completed(bytes_transferred=1_000_000)
        assert completed.status == "completed"
        assert completed.bytes_transferred == 1_000_000
        events = completed.collect_events()
        assert len(events) == 1
        assert events[0].event_type == "TransferJobCompleted"

    def test_mark_failed(self) -> None:
        job = self._make_job()
        failed = job.mark_failed("timeout")
        assert failed.status == "failed"


# ---------------------------------------------------------------------------
# Wave
# ---------------------------------------------------------------------------


class TestWave:
    def _make_wave(self, status: WaveStatus = WaveStatus.PLANNED) -> Wave:
        return Wave(
            wave_id="wave-1",
            name="Wave Alpha",
            lob="finance",
            tables=("t1", "t2", "t3"),
            status=status,
        )

    # -- happy-path transitions --

    def test_start_shadow_run(self) -> None:
        wave = self._make_wave()
        updated = wave.start_shadow_run()
        assert updated.status == WaveStatus.SHADOW_RUNNING
        assert updated.mode == ParallelRunMode.SHADOW

    def test_advance_to_reverse_shadow(self) -> None:
        wave = self._make_wave(WaveStatus.SHADOW_RUNNING)
        updated = wave.advance_to_reverse_shadow()
        assert updated.status == WaveStatus.REVERSE_SHADOW_RUNNING
        assert updated.mode == ParallelRunMode.REVERSE_SHADOW

    def test_cutover(self) -> None:
        wave = self._make_wave(WaveStatus.CUTOVER_READY)
        updated = wave.cutover()
        assert updated.status == WaveStatus.CUTTING_OVER
        assert updated.mode == ParallelRunMode.NEW_ONLY

    def test_rollback_from_shadow_running(self) -> None:
        wave = self._make_wave(WaveStatus.SHADOW_RUNNING)
        rolled = wave.rollback("data mismatch")
        assert rolled.status == WaveStatus.ROLLED_BACK
        assert rolled.mode == ParallelRunMode.OLD_ONLY

    def test_rollback_from_cutting_over(self) -> None:
        wave = self._make_wave(WaveStatus.CUTTING_OVER)
        rolled = wave.rollback("performance regression")
        assert rolled.status == WaveStatus.ROLLED_BACK

    # -- invalid transitions --

    def test_cannot_cutover_from_planned(self) -> None:
        wave = self._make_wave(WaveStatus.PLANNED)
        with pytest.raises(ValueError, match="Cannot transition"):
            wave.cutover()

    def test_cannot_advance_from_completed(self) -> None:
        wave = self._make_wave(WaveStatus.COMPLETED)
        with pytest.raises(ValueError, match="Cannot transition"):
            wave.start_shadow_run()

    def test_cannot_rollback_from_completed(self) -> None:
        wave = self._make_wave(WaveStatus.COMPLETED)
        with pytest.raises(ValueError, match="Cannot transition"):
            wave.rollback("too late")

    def test_cannot_rollback_from_rolled_back(self) -> None:
        wave = self._make_wave(WaveStatus.ROLLED_BACK)
        with pytest.raises(ValueError, match="Cannot transition"):
            wave.rollback("again")

    # -- check_gate --

    def test_check_gate_all_pass(self) -> None:
        wave = self._make_wave(WaveStatus.REVERSE_SHADOW_RUNNING)
        updated, passed = wave.check_gate({"t1": True, "t2": True})
        assert passed is True
        assert updated.status == WaveStatus.CUTOVER_READY

    def test_check_gate_some_fail(self) -> None:
        wave = self._make_wave(WaveStatus.REVERSE_SHADOW_RUNNING)
        updated, passed = wave.check_gate({"t1": True, "t2": False})
        assert passed is False
        assert updated.status == WaveStatus.REVERSE_SHADOW_RUNNING

    def test_check_gate_emits_event_on_pass(self) -> None:
        wave = self._make_wave(WaveStatus.REVERSE_SHADOW_RUNNING)
        updated, _ = wave.check_gate({"t1": True, "t2": True})
        events = updated.collect_events()
        assert len(events) == 1
        assert events[0].event_type == "DVTValidationCompleted"


# ---------------------------------------------------------------------------
# StreamingJob
# ---------------------------------------------------------------------------


class TestStreamingJob:
    def _make_job(self, **overrides: object) -> StreamingJob:
        defaults: dict[str, object] = dict(
            job_id="stream-1",
            source_topic="presto-events",
            target_topic="bq-events",
            consumer_group="cg-1",
        )
        defaults.update(overrides)
        return StreamingJob(**defaults)  # type: ignore[arg-type]

    def test_initiate_drain(self) -> None:
        job = self._make_job()
        drained = job.initiate_drain()
        assert drained.status == "draining"

    def test_switch_target(self) -> None:
        job = self._make_job()
        switched = job.switch_target()
        assert switched.status == "switching"
        events = switched.collect_events()
        assert len(events) == 1
        assert events[0].event_type == "StreamingCutoverInitiated"

    def test_verify_cutover(self) -> None:
        job = self._make_job(current_lag=0)
        verified = job.verify_cutover()
        assert verified.status == "verified"

    def test_verify_cutover_high_lag_raises(self) -> None:
        job = self._make_job(current_lag=5000, lag_threshold=1000)
        with pytest.raises(ValueError, match="exceeds threshold"):
            job.verify_cutover()
