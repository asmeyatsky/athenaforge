from __future__ import annotations

import re

from athenaforge.domain.value_objects.sql_pattern import SqlTranslationPattern


class SqlPatternMatcher:
    """Pure domain service that matches and applies SQL translation patterns."""

    def __init__(self, patterns: list[SqlTranslationPattern]) -> None:
        self._patterns = patterns

    def match_patterns(self, sql: str) -> list[SqlTranslationPattern]:
        """Return all patterns whose ``presto_pattern`` regex matches *sql*."""
        matched: list[SqlTranslationPattern] = []
        for pattern in self._patterns:
            if re.search(pattern.presto_pattern, sql, re.IGNORECASE):
                matched.append(pattern)
        return matched

    def apply_patterns(self, sql: str) -> tuple[str, list[str]]:
        """Apply every matching pattern via ``re.sub``.

        Returns a tuple of (rewritten_sql, list_of_applied_pattern_names).
        """
        rewritten = sql
        applied: list[str] = []
        for pattern in self._patterns:
            new_sql = re.sub(
                pattern.presto_pattern,
                pattern.googlesql_replacement,
                rewritten,
                flags=re.IGNORECASE,
            )
            if new_sql != rewritten:
                applied.append(pattern.name)
                rewritten = new_sql
        return rewritten, applied
