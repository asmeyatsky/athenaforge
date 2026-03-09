from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class TranslationResult:
    """Result of translating a single SQL source file."""

    source_path: str
    translated_sql: str | None
    success: bool
    errors: tuple[str, ...] = ()


class SqlTranslationPort(Protocol):
    """Port for batch SQL dialect translation."""

    async def translate_batch(
        self, source_paths: list[str], output_dir: str
    ) -> list[TranslationResult]: ...
