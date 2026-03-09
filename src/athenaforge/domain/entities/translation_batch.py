from __future__ import annotations

from dataclasses import dataclass, field, replace

from athenaforge.domain.events.event_base import DomainEvent
from athenaforge.domain.events.sql_events import (
    TranslationBatchCompleted,
    TranslationBatchStarted,
)


@dataclass(frozen=True)
class TranslationFile:
    """Immutable record tracking the translation state of a single SQL file."""

    file_path: str
    status: str = "pending"
    error: str | None = None
    translated_path: str | None = None


@dataclass(frozen=True)
class TranslationBatch:
    """Aggregate representing a batch of SQL files undergoing translation."""

    batch_id: str
    files: tuple[TranslationFile, ...]
    status: str = "pending"
    _events: list[DomainEvent] = field(default_factory=list, repr=False)

    # ── commands ────────────────────────────────────────────────

    def mark_file_translated(
        self, file_path: str, translated_path: str
    ) -> TranslationBatch:
        """Return a new batch with the given file marked as translated."""
        updated_files = tuple(
            replace(f, status="translated", translated_path=translated_path)
            if f.file_path == file_path
            else f
            for f in self.files
        )
        new_batch = replace(self, files=updated_files)
        new_batch = new_batch._maybe_complete()
        return new_batch

    def mark_file_failed(self, file_path: str, error: str) -> TranslationBatch:
        """Return a new batch with the given file marked as failed."""
        updated_files = tuple(
            replace(f, status="failed", error=error)
            if f.file_path == file_path
            else f
            for f in self.files
        )
        new_batch = replace(self, files=updated_files)
        new_batch = new_batch._maybe_complete()
        return new_batch

    # ── queries ─────────────────────────────────────────────────

    @property
    def completion_percentage(self) -> float:
        """Percentage of files that are no longer pending."""
        if not self.files:
            return 100.0
        done = sum(1 for f in self.files if f.status != "pending")
        return (done / len(self.files)) * 100.0

    # ── internals ───────────────────────────────────────────────

    def _maybe_complete(self) -> TranslationBatch:
        """If all files are processed, mark the batch as completed and emit event."""
        if all(f.status != "pending" for f in self.files) and self.status != "completed":
            succeeded = sum(1 for f in self.files if f.status == "translated")
            failed = sum(1 for f in self.files if f.status == "failed")
            completed = replace(self, status="completed")
            completed._events.append(
                TranslationBatchCompleted(
                    aggregate_id=self.batch_id,
                    batch_id=self.batch_id,
                    succeeded=succeeded,
                    failed=failed,
                )
            )
            return completed
        return self

    # ── event harvesting ────────────────────────────────────────

    def collect_events(self) -> list[DomainEvent]:
        """Return accumulated events and clear the internal list."""
        events = list(self._events)
        self._events.clear()
        return events
