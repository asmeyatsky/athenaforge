from __future__ import annotations

from typing import Protocol


class BigQueryPort(Protocol):
    """Port for BigQuery operations."""

    async def dry_run(self, query: str) -> int: ...

    async def execute(self, query: str) -> list[dict]: ...

    async def get_table_metadata(self, dataset: str, table: str) -> dict: ...

    async def create_dataset(self, dataset_id: str, location: str) -> None: ...

    async def create_reservation(
        self, reservation_id: str, slots: int, edition: str
    ) -> None: ...
