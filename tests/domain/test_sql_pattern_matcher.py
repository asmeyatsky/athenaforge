from __future__ import annotations

import pytest

from athenaforge.domain.services.sql_pattern_matcher import SqlPatternMatcher
from athenaforge.domain.value_objects.sql_pattern import (
    PatternCategory,
    SqlTranslationPattern,
)

# ---------------------------------------------------------------------------
# Pattern catalogue — one SqlTranslationPattern per category
# ---------------------------------------------------------------------------


def _pat(
    name: str,
    category: PatternCategory,
    presto: str,
    googlesql: str,
    description: str = "",
) -> SqlTranslationPattern:
    return SqlTranslationPattern(
        name=name,
        category=category,
        description=description or name,
        presto_pattern=presto,
        googlesql_replacement=googlesql,
        examples=(),
    )


ALL_PATTERNS: list[SqlTranslationPattern] = [
    _pat(
        "MAP constructor",
        PatternCategory.MAP_CONSTRUCTOR,
        r"MAP\(ARRAY\[([^\]]*)\],\s*ARRAY\[([^\]]*)\]\)",
        r"[STRUCT(\1, \2)]",
    ),
    _pat(
        "TRANSFORM",
        PatternCategory.TRANSFORM,
        r"TRANSFORM\((\w+),\s*(\w+)\s*->\s*([^)]+)\)",
        r"ARRAY(SELECT \3 FROM UNNEST(\1) AS \2)",
    ),
    _pat(
        "FILTER",
        PatternCategory.FILTER,
        r"FILTER\((\w+),\s*(\w+)\s*->\s*([^)]+)\)",
        r"ARRAY(SELECT \2 FROM UNNEST(\1) AS \2 WHERE \3)",
    ),
    _pat(
        "DATE_TRUNC",
        PatternCategory.DATE_TRUNC,
        r"DATE_TRUNC\('(\w+)',\s*(\w+)\)",
        r"DATE_TRUNC(\2, \1)",
    ),
    _pat(
        "DATE_ADD",
        PatternCategory.DATE_ADD,
        r"DATE_ADD\('(\w+)',\s*(\d+),\s*(\w+)\)",
        r"DATE_ADD(\3, INTERVAL \2 \1)",
    ),
    _pat(
        "TRY_CAST",
        PatternCategory.TRY_CAST,
        r"TRY_CAST\((\w+)\s+AS\s+(\w+)\)",
        r"SAFE_CAST(\1 AS \2)",
    ),
    _pat(
        "REGEXP_LIKE",
        PatternCategory.REGEXP_LIKE,
        r"REGEXP_LIKE\((\w+),\s*'([^']+)'\)",
        r"REGEXP_CONTAINS(\1, r'\2')",
    ),
    _pat(
        "approx_distinct",
        PatternCategory.APPROX_DISTINCT,
        r"approx_distinct\((\w+)\)",
        r"APPROX_COUNT_DISTINCT(\1)",
    ),
    _pat(
        "ARBITRARY",
        PatternCategory.ARBITRARY,
        r"ARBITRARY\((\w+)\)",
        r"ANY_VALUE(\1)",
    ),
    _pat(
        "CONTAINS",
        PatternCategory.CONTAINS,
        r"CONTAINS\((\w+),\s*(\w+)\)",
        r"\2 IN UNNEST(\1)",
    ),
    _pat(
        "map_keys",
        PatternCategory.MAP_KEYS,
        r"map_keys\((\w+)\)",
        r"(SELECT ARRAY_AGG(key) FROM UNNEST(\1) AS entry WITH OFFSET AS key)",
    ),
    _pat(
        "map_values",
        PatternCategory.MAP_VALUES,
        r"map_values\((\w+)\)",
        r"(SELECT ARRAY_AGG(value) FROM UNNEST(\1) AS entry WITH OFFSET AS value)",
    ),
]


@pytest.fixture()
def matcher() -> SqlPatternMatcher:
    return SqlPatternMatcher(ALL_PATTERNS)


# ---------------------------------------------------------------------------
# match_patterns tests
# ---------------------------------------------------------------------------


class TestMatchPatterns:
    def test_finds_correct_pattern_for_presto_sql(
        self, matcher: SqlPatternMatcher
    ) -> None:
        sql = "SELECT DATE_TRUNC('month', created_at) FROM t"
        matched = matcher.match_patterns(sql)
        names = [p.name for p in matched]
        assert "DATE_TRUNC" in names

    def test_no_match_for_standard_sql(self, matcher: SqlPatternMatcher) -> None:
        sql = "SELECT id, name FROM users WHERE active = TRUE"
        matched = matcher.match_patterns(sql)
        assert matched == []


# ---------------------------------------------------------------------------
# apply_patterns — one test per category
# ---------------------------------------------------------------------------


class TestApplyPatterns:
    def test_map_constructor(self, matcher: SqlPatternMatcher) -> None:
        sql = "SELECT MAP(ARRAY['a'], ARRAY[1]) FROM t"
        rewritten, applied = matcher.apply_patterns(sql)
        assert "MAP constructor" in applied
        assert "STRUCT" in rewritten
        assert "MAP(ARRAY" not in rewritten

    def test_transform(self, matcher: SqlPatternMatcher) -> None:
        sql = "SELECT TRANSFORM(arr, x -> x + 1) FROM t"
        rewritten, applied = matcher.apply_patterns(sql)
        assert "TRANSFORM" in applied
        assert "UNNEST" in rewritten

    def test_filter(self, matcher: SqlPatternMatcher) -> None:
        sql = "SELECT FILTER(arr, x -> x > 0) FROM t"
        rewritten, applied = matcher.apply_patterns(sql)
        assert "FILTER" in applied
        assert "WHERE" in rewritten

    def test_date_trunc(self, matcher: SqlPatternMatcher) -> None:
        sql = "SELECT DATE_TRUNC('month', dt) FROM t"
        rewritten, applied = matcher.apply_patterns(sql)
        assert "DATE_TRUNC" in applied
        assert "DATE_TRUNC(dt, month)" in rewritten

    def test_date_add(self, matcher: SqlPatternMatcher) -> None:
        sql = "SELECT DATE_ADD('day', 7, dt) FROM t"
        rewritten, applied = matcher.apply_patterns(sql)
        assert "DATE_ADD" in applied
        assert "INTERVAL 7 day" in rewritten

    def test_try_cast(self, matcher: SqlPatternMatcher) -> None:
        sql = "SELECT TRY_CAST(x AS INT) FROM t"
        rewritten, applied = matcher.apply_patterns(sql)
        assert "TRY_CAST" in applied
        assert "SAFE_CAST" in rewritten

    def test_regexp_like(self, matcher: SqlPatternMatcher) -> None:
        sql = "SELECT REGEXP_LIKE(col, 'pattern') FROM t"
        rewritten, applied = matcher.apply_patterns(sql)
        assert "REGEXP_LIKE" in applied
        assert "REGEXP_CONTAINS" in rewritten

    def test_approx_distinct(self, matcher: SqlPatternMatcher) -> None:
        sql = "SELECT approx_distinct(col) FROM t"
        rewritten, applied = matcher.apply_patterns(sql)
        assert "approx_distinct" in applied
        assert "APPROX_COUNT_DISTINCT" in rewritten

    def test_arbitrary(self, matcher: SqlPatternMatcher) -> None:
        sql = "SELECT ARBITRARY(col) FROM t"
        rewritten, applied = matcher.apply_patterns(sql)
        assert "ARBITRARY" in applied
        assert "ANY_VALUE" in rewritten

    def test_contains(self, matcher: SqlPatternMatcher) -> None:
        sql = "SELECT CONTAINS(arr, val) FROM t"
        rewritten, applied = matcher.apply_patterns(sql)
        assert "CONTAINS" in applied
        assert "IN UNNEST" in rewritten

    def test_map_keys(self, matcher: SqlPatternMatcher) -> None:
        sql = "SELECT map_keys(m) FROM t"
        rewritten, applied = matcher.apply_patterns(sql)
        assert "map_keys" in applied
        assert "ARRAY_AGG" in rewritten

    def test_map_values(self, matcher: SqlPatternMatcher) -> None:
        sql = "SELECT map_values(m) FROM t"
        rewritten, applied = matcher.apply_patterns(sql)
        assert "map_values" in applied
        assert "ARRAY_AGG" in rewritten


class TestMultiPattern:
    def test_sql_with_multiple_patterns_all_rewritten(
        self, matcher: SqlPatternMatcher
    ) -> None:
        sql = (
            "SELECT approx_distinct(col), ARBITRARY(col2), "
            "TRY_CAST(x AS INT) FROM t"
        )
        rewritten, applied = matcher.apply_patterns(sql)
        assert "approx_distinct" in applied
        assert "ARBITRARY" in applied
        assert "TRY_CAST" in applied
        assert "APPROX_COUNT_DISTINCT" in rewritten
        assert "ANY_VALUE" in rewritten
        assert "SAFE_CAST" in rewritten


class TestNoMatch:
    def test_standard_sql_passes_through_unchanged(
        self, matcher: SqlPatternMatcher
    ) -> None:
        sql = "SELECT id, name FROM users WHERE active = TRUE ORDER BY id"
        rewritten, applied = matcher.apply_patterns(sql)
        assert rewritten == sql
        assert applied == []
