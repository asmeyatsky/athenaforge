from __future__ import annotations

import re
from pathlib import Path

import pytest

from athenaforge.domain.value_objects.sql_pattern import (
    PatternCategory,
    SqlTranslationPattern,
)
from athenaforge.infrastructure.adapters import PatternLoader


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_PATTERNS_FILE = str(
    Path(__file__).resolve().parents[2]
    / "src"
    / "athenaforge"
    / "infrastructure"
    / "patterns"
    / "presto_patterns.yaml"
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def loader() -> PatternLoader:
    return PatternLoader()


@pytest.fixture()
def patterns(loader: PatternLoader) -> list[SqlTranslationPattern]:
    return loader.load(_PATTERNS_FILE)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPatternLoaderBasics:
    """Verify that the loader reads the actual presto_patterns.yaml correctly."""

    def test_load_returns_list(self, patterns: list[SqlTranslationPattern]) -> None:
        assert isinstance(patterns, list)
        assert len(patterns) > 0

    def test_loaded_patterns_are_correct_type(
        self, patterns: list[SqlTranslationPattern]
    ) -> None:
        for p in patterns:
            assert isinstance(p, SqlTranslationPattern)


class TestPatternCategories:
    """Verify category coverage matches what is in the YAML file."""

    def test_at_least_12_categories_represented(
        self, patterns: list[SqlTranslationPattern]
    ) -> None:
        categories = {p.category for p in patterns}
        assert len(categories) >= 12, (
            f"Expected at least 12 categories, found {len(categories)}: {categories}"
        )

    def test_known_categories_present(
        self, patterns: list[SqlTranslationPattern]
    ) -> None:
        categories = {p.category for p in patterns}
        expected = {
            PatternCategory.MAP_CONSTRUCTOR,
            PatternCategory.TRANSFORM,
            PatternCategory.FILTER,
            PatternCategory.DATE_TRUNC,
            PatternCategory.DATE_ADD,
            PatternCategory.TRY_CAST,
            PatternCategory.REGEXP_LIKE,
            PatternCategory.APPROX_DISTINCT,
            PatternCategory.ARBITRARY,
            PatternCategory.CONTAINS,
            PatternCategory.MAP_KEYS,
            PatternCategory.MAP_VALUES,
        }
        missing = expected - categories
        assert not missing, f"Missing expected categories: {missing}"


class TestPatternExamples:
    """Every pattern should have at least one example."""

    def test_each_pattern_has_examples(
        self, patterns: list[SqlTranslationPattern]
    ) -> None:
        for p in patterns:
            assert len(p.examples) >= 1, (
                f"Pattern '{p.name}' has no examples"
            )

    def test_examples_have_both_fields(
        self, patterns: list[SqlTranslationPattern]
    ) -> None:
        for p in patterns:
            for ex in p.examples:
                assert ex.presto_sql, (
                    f"Pattern '{p.name}' has an example with empty presto_sql"
                )
                assert ex.googlesql, (
                    f"Pattern '{p.name}' has an example with empty googlesql"
                )


class TestPatternRegexValidity:
    """Pattern regex fields should compile without errors."""

    def test_presto_pattern_regex_compiles(
        self, patterns: list[SqlTranslationPattern]
    ) -> None:
        for p in patterns:
            try:
                re.compile(p.presto_pattern)
            except re.error as exc:
                pytest.fail(
                    f"Pattern '{p.name}' has invalid regex in presto_pattern: "
                    f"'{p.presto_pattern}' -- {exc}"
                )


class TestPatternFields:
    """Each pattern should have required non-empty fields."""

    def test_each_pattern_has_name(
        self, patterns: list[SqlTranslationPattern]
    ) -> None:
        for p in patterns:
            assert p.name, "Found a pattern with an empty name"

    def test_each_pattern_has_description(
        self, patterns: list[SqlTranslationPattern]
    ) -> None:
        for p in patterns:
            assert p.description, (
                f"Pattern '{p.name}' has an empty description"
            )

    def test_each_pattern_has_googlesql_replacement(
        self, patterns: list[SqlTranslationPattern]
    ) -> None:
        for p in patterns:
            assert p.googlesql_replacement, (
                f"Pattern '{p.name}' has an empty googlesql_replacement"
            )
