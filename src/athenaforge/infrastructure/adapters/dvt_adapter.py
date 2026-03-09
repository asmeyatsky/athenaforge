from __future__ import annotations


class DvtAdapter:
    """Implements DVTPort with placeholder stubs.

    All methods return a dict containing a ``status`` of ``"pass"`` and
    summary information.  A real implementation would invoke the
    ``google-pso-data-validator`` CLI or library.
    """

    async def validate_row_count(
        self, source_table: str, target_table: str
    ) -> dict:
        return {
            "validation_type": "row_count",
            "source_table": source_table,
            "target_table": target_table,
            "source_count": 0,
            "target_count": 0,
            "status": "pass",
        }

    async def validate_column_aggregates(
        self, source_table: str, target_table: str, columns: list[str]
    ) -> dict:
        return {
            "validation_type": "column_aggregates",
            "source_table": source_table,
            "target_table": target_table,
            "columns": columns,
            "status": "pass",
        }

    async def validate_row_hash(
        self, source_table: str, target_table: str, primary_keys: list[str]
    ) -> dict:
        return {
            "validation_type": "row_hash",
            "source_table": source_table,
            "target_table": target_table,
            "primary_keys": primary_keys,
            "status": "pass",
        }
