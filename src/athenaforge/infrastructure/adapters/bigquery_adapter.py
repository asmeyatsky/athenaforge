from __future__ import annotations


class BigQueryAdapter:
    """Implements BigQueryPort.

    All methods except ``dry_run`` are stubs that raise
    ``NotImplementedError`` until real GCP credentials are available.
    """

    def __init__(self, project_id: str) -> None:
        self._project_id = project_id

    async def dry_run(self, query: str) -> int:
        """Return a placeholder bytes-processed estimate."""
        return len(query.encode("utf-8")) * 10

    async def execute(self, query: str) -> list[dict]:
        raise NotImplementedError("Requires GCP credentials")

    async def get_table_metadata(self, dataset: str, table: str) -> dict:
        raise NotImplementedError("Requires GCP credentials")

    async def create_dataset(self, dataset_id: str, location: str) -> None:
        raise NotImplementedError("Requires GCP credentials")

    async def create_reservation(
        self, reservation_id: str, slots: int, edition: str
    ) -> None:
        raise NotImplementedError("Requires GCP credentials")
