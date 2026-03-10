from __future__ import annotations

import re

from athenaforge.application.dtos.sql_dtos import CaseNormalisationResult


class NormaliseCaseSensitivityUseCase:
    """Wraps column references in UPPER() to normalise case sensitivity."""

    async def execute(self, sql_content: str, columns: list[str]) -> CaseNormalisationResult:
        """For each column in *columns*, wrap bare references in ``UPPER()`` calls.

        This ensures case-insensitive comparison behaviour when migrating
        from a case-insensitive source system to BigQuery.
        """
        result = sql_content
        columns_normalised = 0
        for column in columns:
            # Match the column name as a whole word, but not when already
            # wrapped in an UPPER() call.  The negative lookbehind prevents
            # nesting UPPER(UPPER(...)).
            pattern = re.compile(
                rf"(?<![A-Za-z_])(?<!UPPER\()({re.escape(column)})(?![A-Za-z_])",
                re.IGNORECASE,
            )
            result, count = pattern.subn(rf"UPPER(\1)", result)
            columns_normalised += count
        return CaseNormalisationResult(
            original_sql=sql_content,
            normalised_sql=result,
            columns_normalised=columns_normalised,
        )
