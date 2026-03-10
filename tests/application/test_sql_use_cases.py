"""Application-layer tests for SQL module use cases."""

from __future__ import annotations

import pytest

from athenaforge.application.commands.sql.analyse_map_cascade import (
    AnalyseMapCascadeUseCase,
)
from athenaforge.application.commands.sql.classify_udfs import ClassifyUDFsUseCase
from athenaforge.application.commands.sql.normalise_case_sensitivity import (
    NormaliseCaseSensitivityUseCase,
)
from athenaforge.application.commands.sql.validate_queries import (
    ValidateQueriesUseCase,
)
from athenaforge.domain.events.event_base import DomainEvent
from athenaforge.domain.events.sql_events import (
    MapCascadeAnalysed,
    QueryValidationFailed,
    QueryValidationPassed,
)
from athenaforge.domain.services.map_cascade_analyser import MapCascadeAnalyser
from athenaforge.domain.services.udf_classifier import UDFClassifier


# ── Stub ports ───────────────────────────────────────────────────────────────


class StubEventBus:
    """Collects all published events."""

    def __init__(self) -> None:
        self.events: list[DomainEvent] = []

    async def publish(self, event: DomainEvent) -> None:
        self.events.append(event)

    def subscribe(self, event_type: type, handler: object) -> None:
        pass


class StubBigQueryPort:
    """Simulates BigQuery dry-run with configurable results and failures."""

    def __init__(
        self,
        dry_run_results: dict[str, int] | None = None,
        failures: set[str] | None = None,
    ) -> None:
        self._results = dry_run_results or {}
        self._failures = failures or set()

    async def dry_run(self, query: str) -> int:
        if query in self._failures:
            raise Exception(f"Invalid query: {query}")
        return self._results.get(query, 1000)

    async def execute(self, query: str) -> list[dict]:
        raise NotImplementedError

    async def get_table_metadata(self, dataset: str, table: str) -> dict:
        raise NotImplementedError

    async def create_dataset(self, dataset_id: str, location: str) -> None:
        raise NotImplementedError

    async def create_reservation(
        self, reservation_id: str, slots: int, edition: str
    ) -> None:
        raise NotImplementedError


# ── AnalyseMapCascadeUseCase tests ───────────────────────────────────────────


async def test_analyse_map_cascade_calculates_depth_for_chain():
    """A linear chain A -> B -> C should report cascade depth of 2."""
    analyser = MapCascadeAnalyser()
    bus = StubEventBus()

    dependencies = {
        "A": ["B"],
        "B": ["C"],
    }

    uc = AnalyseMapCascadeUseCase(analyser, bus)
    result = await uc.execute(dependencies)

    assert result.cascade_depth == 2
    assert result.total_maps == 1  # only root "A" is analysed


async def test_analyse_map_cascade_identifies_co_migration_batches():
    """Connected components should be returned as co-migration batches."""
    analyser = MapCascadeAnalyser()
    bus = StubEventBus()

    # Two disconnected groups: {A, B} and {C, D}
    dependencies = {
        "A": ["B"],
        "C": ["D"],
    }

    uc = AnalyseMapCascadeUseCase(analyser, bus)
    result = await uc.execute(dependencies)

    assert len(result.co_migration_batches) == 2
    batch_sets = [set(batch) for batch in result.co_migration_batches]
    assert {"A", "B"} in batch_sets
    assert {"C", "D"} in batch_sets


async def test_analyse_map_cascade_publishes_event():
    """A MapCascadeAnalysed event must be published."""
    analyser = MapCascadeAnalyser()
    bus = StubEventBus()

    dependencies = {"X": ["Y"]}

    uc = AnalyseMapCascadeUseCase(analyser, bus)
    await uc.execute(dependencies)

    cascade_events = [e for e in bus.events if isinstance(e, MapCascadeAnalysed)]
    assert len(cascade_events) == 1
    assert cascade_events[0].aggregate_id == "map-cascade"
    assert cascade_events[0].total_maps >= 1


async def test_analyse_map_cascade_handles_empty_graph():
    """An empty dependency graph should return zero maps and depth."""
    analyser = MapCascadeAnalyser()
    bus = StubEventBus()

    uc = AnalyseMapCascadeUseCase(analyser, bus)
    result = await uc.execute({})

    assert result.total_maps == 0
    assert result.cascade_depth == 0
    assert result.co_migration_batches == []


# ── NormaliseCaseSensitivityUseCase tests ────────────────────────────────────


async def test_normalise_wraps_columns_in_upper():
    """Bare column references should be wrapped with UPPER()."""
    uc = NormaliseCaseSensitivityUseCase()

    sql = "SELECT user_name, email FROM users WHERE user_name = 'alice'"
    result = await uc.execute(sql, ["user_name", "email"])

    assert "UPPER(user_name)" in result.normalised_sql
    assert "UPPER(email)" in result.normalised_sql
    assert result.original_sql == sql


async def test_normalise_avoids_double_wrapping():
    """Columns already inside UPPER() should not be double-wrapped."""
    uc = NormaliseCaseSensitivityUseCase()

    sql = "SELECT UPPER(city) FROM addresses"
    result = await uc.execute(sql, ["city"])

    # Should not produce UPPER(UPPER(city))
    assert "UPPER(UPPER(" not in result.normalised_sql


async def test_normalise_returns_correct_count():
    """The columns_normalised count should reflect actual substitutions made."""
    uc = NormaliseCaseSensitivityUseCase()

    sql = "SELECT name, name FROM t WHERE name = 'x'"
    result = await uc.execute(sql, ["name"])

    # "name" appears 3 times, all should be wrapped
    assert result.columns_normalised == 3


async def test_normalise_handles_no_matching_columns():
    """SQL with no matching columns should return the original SQL unchanged."""
    uc = NormaliseCaseSensitivityUseCase()

    sql = "SELECT id, amount FROM orders"
    result = await uc.execute(sql, ["nonexistent_column"])

    assert result.normalised_sql == sql
    assert result.columns_normalised == 0


# ── ClassifyUDFsUseCase tests ───────────────────────────────────────────────


async def test_classify_udfs_sql():
    """UDFs containing only SQL constructs should be classified as SQL_UDF."""
    classifier = UDFClassifier()

    uc = ClassifyUDFsUseCase(classifier)
    result = await uc.execute({
        "my_sum": "RETURN SELECT SUM(x) FROM t",
        "add_one": "RETURN x + 1",
    })

    assert result.sql_udfs == 2
    assert result.js_udfs == 0
    assert result.cloud_run_udfs == 0
    assert result.classifications["my_sum"] == "SQL_UDF"
    assert result.classifications["add_one"] == "SQL_UDF"


async def test_classify_udfs_javascript():
    """UDFs with JavaScript patterns should be classified as JS_UDF."""
    classifier = UDFClassifier()

    uc = ClassifyUDFsUseCase(classifier)
    result = await uc.execute({
        "parse_json": "var data = JSON.parse(input); return data.value;",
        "arrow_fn": "const double = (x) => x * 2; return double(val);",
    })

    assert result.js_udfs == 2
    assert result.sql_udfs == 0
    assert result.classifications["parse_json"] == "JS_UDF"
    assert result.classifications["arrow_fn"] == "JS_UDF"


async def test_classify_udfs_cloud_run():
    """UDFs with HTTP calls or external imports should be classified as CLOUD_RUN_REMOTE."""
    classifier = UDFClassifier()

    uc = ClassifyUDFsUseCase(classifier)
    result = await uc.execute({
        "remote_call": "import requests; return requests.get(https://api.example.com/data)",
        "java_udf": "public class MyUDF { public String process(String input) {} }",
    })

    assert result.cloud_run_udfs == 2
    assert result.classifications["remote_call"] == "CLOUD_RUN_REMOTE"
    assert result.classifications["java_udf"] == "CLOUD_RUN_REMOTE"


async def test_classify_udfs_empty_dict():
    """An empty UDF dictionary should return zero counts."""
    classifier = UDFClassifier()

    uc = ClassifyUDFsUseCase(classifier)
    result = await uc.execute({})

    assert result.total_udfs == 0
    assert result.sql_udfs == 0
    assert result.js_udfs == 0
    assert result.cloud_run_udfs == 0
    assert result.classifications == {}


# ── ValidateQueriesUseCase tests ─────────────────────────────────────────────


async def test_validate_queries_all_pass():
    """When all queries pass dry-run, the report should show zero failures."""
    bus = StubEventBus()
    bq = StubBigQueryPort(
        dry_run_results={
            "SELECT 1": 500,
            "SELECT 2": 700,
        }
    )

    uc = ValidateQueriesUseCase(bq, bus)
    result = await uc.execute(
        query_paths=["q1.sql", "q2.sql"],
        query_contents={"q1.sql": "SELECT 1", "q2.sql": "SELECT 2"},
    )

    assert result.total_queries == 2
    assert result.passed == 2
    assert result.failed == 0
    assert result.failures == []


async def test_validate_queries_some_fail():
    """When some queries fail, the report should reflect correct pass/fail counts."""
    bus = StubEventBus()
    bq = StubBigQueryPort(
        dry_run_results={"SELECT 1": 1000},
        failures={"BAD QUERY"},
    )

    uc = ValidateQueriesUseCase(bq, bus)
    result = await uc.execute(
        query_paths=["good.sql", "bad.sql"],
        query_contents={"good.sql": "SELECT 1", "bad.sql": "BAD QUERY"},
    )

    assert result.total_queries == 2
    assert result.passed == 1
    assert result.failed == 1
    assert len(result.failures) == 1
    assert result.failures[0]["query_path"] == "bad.sql"
    assert "Invalid query" in result.failures[0]["error"]


async def test_validate_queries_publishes_pass_and_fail_events():
    """QueryValidationPassed and QueryValidationFailed events must be published."""
    bus = StubEventBus()
    bq = StubBigQueryPort(
        dry_run_results={"SELECT 1": 2000},
        failures={"INVALID"},
    )

    uc = ValidateQueriesUseCase(bq, bus)
    await uc.execute(
        query_paths=["ok.sql", "err.sql"],
        query_contents={"ok.sql": "SELECT 1", "err.sql": "INVALID"},
    )

    passed_events = [e for e in bus.events if isinstance(e, QueryValidationPassed)]
    failed_events = [e for e in bus.events if isinstance(e, QueryValidationFailed)]

    assert len(passed_events) == 1
    assert passed_events[0].query_path == "ok.sql"
    assert passed_events[0].dry_run_bytes == 2000

    assert len(failed_events) == 1
    assert failed_events[0].query_path == "err.sql"
    assert "Invalid query" in failed_events[0].error_message


async def test_validate_queries_returns_correct_bytes_scanned():
    """The total_bytes_scanned should be the sum of all successful dry-runs."""
    bus = StubEventBus()
    bq = StubBigQueryPort(
        dry_run_results={
            "SELECT a": 3000,
            "SELECT b": 5000,
            "SELECT c": 2000,
        }
    )

    uc = ValidateQueriesUseCase(bq, bus)
    result = await uc.execute(
        query_paths=["a.sql", "b.sql", "c.sql"],
        query_contents={
            "a.sql": "SELECT a",
            "b.sql": "SELECT b",
            "c.sql": "SELECT c",
        },
    )

    assert result.total_bytes_scanned == 10_000
    assert result.passed == 3
    assert result.failed == 0
