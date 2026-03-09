from __future__ import annotations

from typing import Protocol


class DVTPort(Protocol):
    """Port for Data Validation Tool operations."""

    async def validate_row_count(
        self, source_table: str, target_table: str
    ) -> dict: ...

    async def validate_column_aggregates(
        self, source_table: str, target_table: str, columns: list[str]
    ) -> dict: ...

    async def validate_row_hash(
        self, source_table: str, target_table: str, primary_keys: list[str]
    ) -> dict: ...
