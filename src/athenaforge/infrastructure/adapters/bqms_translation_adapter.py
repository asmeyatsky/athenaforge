from __future__ import annotations

from pathlib import Path

from athenaforge.domain.ports.sql_translation_port import (
    SqlTranslationPort,
    TranslationResult,
)


class BqmsTranslationAdapter:
    """Implements SqlTranslationPort using the BigQuery Migration Service.

    Currently a stub that returns source SQL as-is.  A real implementation
    would call the BigQuery Migration API for Presto-to-GoogleSQL
    translation.
    """

    def __init__(self, project_id: str, location: str) -> None:
        self._project_id = project_id
        self._location = location

    async def translate_batch(
        self, source_paths: list[str], output_dir: str
    ) -> list[TranslationResult]:
        results: list[TranslationResult] = []
        for source_path in source_paths:
            try:
                sql = Path(source_path).read_text(encoding="utf-8")
                results.append(
                    TranslationResult(
                        source_path=source_path,
                        translated_sql=sql,
                        success=True,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                results.append(
                    TranslationResult(
                        source_path=source_path,
                        translated_sql=None,
                        success=False,
                        errors=(str(exc),),
                    )
                )
        return results
