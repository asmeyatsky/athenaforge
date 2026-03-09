from __future__ import annotations

import re

from athenaforge.domain.ports.event_bus import EventBusPort


class NormaliseCaseSensitivityUseCase:
    """Wraps column references in UPPER() to normalise case sensitivity."""

    def __init__(self, event_bus: EventBusPort) -> None:
        self._event_bus = event_bus

    async def execute(self, sql_content: str, columns: list[str]) -> str:
        """For each column in *columns*, wrap bare references in ``UPPER()`` calls.

        This ensures case-insensitive comparison behaviour when migrating
        from a case-insensitive source system to BigQuery.
        """
        result = sql_content
        for column in columns:
            # Match the column name as a whole word, but not when already
            # wrapped in an UPPER() call.
            pattern = re.compile(
                rf"(?<!\bUPPER\()(?<!\w)({re.escape(column)})(?!\w)",
                re.IGNORECASE,
            )
            result = pattern.sub(rf"UPPER(\1)", result)
        return result
