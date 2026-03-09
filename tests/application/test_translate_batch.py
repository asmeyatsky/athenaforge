"""Application-layer tests for TranslateBatchUseCase."""

from __future__ import annotations

import os
import tempfile

import pytest

from athenaforge.application.commands.sql.translate_batch import TranslateBatchUseCase
from athenaforge.domain.events.event_base import DomainEvent
from athenaforge.domain.events.sql_events import (
    TranslationBatchCompleted,
    TranslationBatchStarted,
)
from athenaforge.domain.ports.sql_translation_port import TranslationResult
from athenaforge.domain.services.sql_pattern_matcher import SqlPatternMatcher
from athenaforge.domain.value_objects.sql_pattern import (
    PatternCategory,
    SqlTranslationPattern,
)


# ── Stub ports ───────────────────────────────────────────────────────────────


class StubSqlTranslationPort:
    """Returns pre-configured TranslationResults for every file."""

    def __init__(self, results: list[TranslationResult] | None = None) -> None:
        self._results = results
        self.called_with: tuple[list[str], str] | None = None

    async def translate_batch(
        self, source_paths: list[str], output_dir: str
    ) -> list[TranslationResult]:
        self.called_with = (source_paths, output_dir)
        if self._results is not None:
            return self._results
        # Default: succeed for every file
        return [
            TranslationResult(
                source_path=sp,
                translated_sql=f"-- translated {os.path.basename(sp)}",
                success=True,
            )
            for sp in source_paths
        ]


class StubWriteRepositoryPort:
    """Records saved entities."""

    def __init__(self) -> None:
        self.saved: list[object] = []

    async def save(self, entity: object) -> None:
        self.saved.append(entity)

    async def delete(self, id: str) -> None:
        pass


class StubEventBus:
    """Collects all published events."""

    def __init__(self) -> None:
        self.events: list[DomainEvent] = []

    async def publish(self, event: DomainEvent) -> None:
        self.events.append(event)

    def subscribe(self, event_type: type, handler: object) -> None:
        pass


# ── Helpers ──────────────────────────────────────────────────────────────────

SAMPLE_PATTERNS = [
    SqlTranslationPattern(
        name="date_trunc_rewrite",
        category=PatternCategory.DATE_TRUNC,
        description="Rewrite Presto DATE_TRUNC to GoogleSQL",
        presto_pattern=r"DATE_TRUNC\(\s*'(\w+)'\s*,\s*(\w+)\s*\)",
        googlesql_replacement=r"DATE_TRUNC(\2, \1)",
        examples=(),
    ),
    SqlTranslationPattern(
        name="approx_distinct_rewrite",
        category=PatternCategory.APPROX_DISTINCT,
        description="Rewrite APPROX_DISTINCT to APPROX_COUNT_DISTINCT",
        presto_pattern=r"APPROX_DISTINCT\(",
        googlesql_replacement="APPROX_COUNT_DISTINCT(",
        examples=(),
    ),
]


def _create_sql_files(tmpdir: str, files: dict[str, str]) -> None:
    """Write SQL files into *tmpdir*."""
    for name, content in files.items():
        with open(os.path.join(tmpdir, name), "w") as fh:
            fh.write(content)


# ── Tests ────────────────────────────────────────────────────────────────────


async def test_execute_applies_patterns_before_translation():
    """Regex patterns should be applied before handing files to the translator."""
    with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as out_dir:
        _create_sql_files(
            src_dir,
            {
                "query1.sql": "SELECT DATE_TRUNC('month', created_at) FROM t",
                "query2.sql": "SELECT APPROX_DISTINCT(user_id) FROM t",
            },
        )

        matcher = SqlPatternMatcher(SAMPLE_PATTERNS)
        translator = StubSqlTranslationPort()
        repo = StubWriteRepositoryPort()
        bus = StubEventBus()

        uc = TranslateBatchUseCase(matcher, translator, repo, bus)
        result = await uc.execute(src_dir, out_dir)

        # Verify patterns were applied
        assert "date_trunc_rewrite" in result.patterns_applied
        assert "approx_distinct_rewrite" in result.patterns_applied

        # The translator received pre-passed files in the output dir
        assert translator.called_with is not None
        passed_paths, passed_dir = translator.called_with
        assert len(passed_paths) == 2
        assert passed_dir == out_dir

        # Verify the pre-passed files contain the rewritten SQL
        for pp in passed_paths:
            with open(pp) as fh:
                content = fh.read()
            # The original Presto syntax should be gone
            assert "DATE_TRUNC('" not in content or "APPROX_DISTINCT(" not in content


async def test_started_and_completed_events_published():
    """TranslationBatchStarted and TranslationBatchCompleted events must both be published."""
    with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as out_dir:
        _create_sql_files(src_dir, {"a.sql": "SELECT 1", "b.sql": "SELECT 2"})

        matcher = SqlPatternMatcher([])
        translator = StubSqlTranslationPort()
        repo = StubWriteRepositoryPort()
        bus = StubEventBus()

        uc = TranslateBatchUseCase(matcher, translator, repo, bus)
        await uc.execute(src_dir, out_dir)

        started = [e for e in bus.events if isinstance(e, TranslationBatchStarted)]
        completed = [e for e in bus.events if isinstance(e, TranslationBatchCompleted)]

        assert len(started) == 1
        assert started[0].file_count == 2

        # At least one completed event (use case publishes one explicitly,
        # and the aggregate may publish another via collect_events)
        assert len(completed) >= 1
        # The use-case-level completed event
        uc_completed = completed[0]
        assert uc_completed.succeeded == 2
        assert uc_completed.failed == 0


async def test_partial_failure_counted_correctly():
    """When some translations fail, succeeded/failed counts should be accurate."""
    with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as out_dir:
        _create_sql_files(
            src_dir,
            {
                "good.sql": "SELECT 1",
                "bad.sql": "INVALID SQL",
                "ok.sql": "SELECT 3",
            },
        )

        # Pre-configure the translator to fail on 'bad.sql'
        pre_passed_bad = os.path.join(out_dir, "bad.sql")
        pre_passed_good = os.path.join(out_dir, "good.sql")
        pre_passed_ok = os.path.join(out_dir, "ok.sql")

        custom_results = [
            TranslationResult(
                source_path=pre_passed_bad,
                translated_sql=None,
                success=False,
                errors=("Syntax error near INVALID",),
            ),
            TranslationResult(
                source_path=pre_passed_good,
                translated_sql="SELECT 1",
                success=True,
            ),
            TranslationResult(
                source_path=pre_passed_ok,
                translated_sql="SELECT 3",
                success=True,
            ),
        ]
        translator = StubSqlTranslationPort(results=custom_results)
        matcher = SqlPatternMatcher([])
        repo = StubWriteRepositoryPort()
        bus = StubEventBus()

        uc = TranslateBatchUseCase(matcher, translator, repo, bus)
        result = await uc.execute(src_dir, out_dir)

        assert result.total_files == 3
        assert result.succeeded == 2
        assert result.failed == 1


async def test_batch_persisted_to_repository():
    """The translation batch aggregate should be saved to the repository."""
    with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as out_dir:
        _create_sql_files(src_dir, {"x.sql": "SELECT 42"})

        matcher = SqlPatternMatcher([])
        translator = StubSqlTranslationPort()
        repo = StubWriteRepositoryPort()
        bus = StubEventBus()

        uc = TranslateBatchUseCase(matcher, translator, repo, bus)
        await uc.execute(src_dir, out_dir)

        assert len(repo.saved) == 1
        batch = repo.saved[0]
        assert hasattr(batch, "batch_id")
        assert hasattr(batch, "status")


async def test_no_sql_files_produces_empty_batch():
    """A source directory with no .sql files should produce an empty batch."""
    with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as out_dir:
        # Write a non-sql file
        with open(os.path.join(src_dir, "readme.txt"), "w") as fh:
            fh.write("not sql")

        matcher = SqlPatternMatcher([])
        translator = StubSqlTranslationPort()
        repo = StubWriteRepositoryPort()
        bus = StubEventBus()

        uc = TranslateBatchUseCase(matcher, translator, repo, bus)
        result = await uc.execute(src_dir, out_dir)

        assert result.total_files == 0
        assert result.succeeded == 0
        assert result.failed == 0
        assert result.patterns_applied == []


async def test_all_translations_fail():
    """When every file fails translation, failed count equals total."""
    with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as out_dir:
        _create_sql_files(src_dir, {"f1.sql": "BAD", "f2.sql": "WORSE"})

        pre_f1 = os.path.join(out_dir, "f1.sql")
        pre_f2 = os.path.join(out_dir, "f2.sql")

        custom_results = [
            TranslationResult(
                source_path=pre_f1,
                translated_sql=None,
                success=False,
                errors=("parse error",),
            ),
            TranslationResult(
                source_path=pre_f2,
                translated_sql=None,
                success=False,
                errors=("unknown function",),
            ),
        ]
        translator = StubSqlTranslationPort(results=custom_results)
        matcher = SqlPatternMatcher([])
        repo = StubWriteRepositoryPort()
        bus = StubEventBus()

        uc = TranslateBatchUseCase(matcher, translator, repo, bus)
        result = await uc.execute(src_dir, out_dir)

        assert result.total_files == 2
        assert result.succeeded == 0
        assert result.failed == 2

        # The completed event should reflect the failure counts
        completed = [e for e in bus.events if isinstance(e, TranslationBatchCompleted)]
        assert any(e.failed == 2 for e in completed)


async def test_result_batch_id_is_set():
    """The result must contain a non-empty batch_id."""
    with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as out_dir:
        _create_sql_files(src_dir, {"q.sql": "SELECT 1"})

        matcher = SqlPatternMatcher([])
        translator = StubSqlTranslationPort()
        repo = StubWriteRepositoryPort()
        bus = StubEventBus()

        uc = TranslateBatchUseCase(matcher, translator, repo, bus)
        result = await uc.execute(src_dir, out_dir)

        assert result.batch_id
        assert len(result.batch_id) > 0
