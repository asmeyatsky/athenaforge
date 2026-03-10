from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Self

from athenaforge.domain.events.event_base import DomainEvent
from athenaforge.domain.events.sql_events import TranslationBatchCompleted
from athenaforge.domain.value_objects.status import BatchStatus, FileStatus


@dataclass(frozen=True)
class TranslationFile:
    """Immutable record tracking the translation state of a single SQL file."""

    file_path: str
    status: FileStatus = FileStatus.PENDING
    error: str | None = None
    translated_path: str | None = None


@dataclass(frozen=True)
class TranslationBatch:
    """Aggregate representing a batch of SQL files undergoing translation."""

    batch_id: str
    files: tuple[TranslationFile, ...]
    status: BatchStatus = BatchStatus.PENDING
    _events: tuple[DomainEvent, ...] = field(default=(), repr=False)

    # ── commands ────────────────────────────────────────────────

    def mark_file_translated(
        self, file_path: str, translated_path: str
    ) -> TranslationBatch:
        """Return a new batch with the given file marked as translated."""
        updated_files = tuple(
            replace(f, status=FileStatus.TRANSLATED, translated_path=translated_path)
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
            replace(f, status=FileStatus.FAILED, error=error)
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
        done = sum(1 for f in self.files if f.status != FileStatus.PENDING)
        return (done / len(self.files)) * 100.0

    # ── internals ───────────────────────────────────────────────

    def _maybe_complete(self) -> TranslationBatch:
        """If all files are processed, mark the batch as completed and emit event."""
        if all(f.status != FileStatus.PENDING for f in self.files) and self.status != BatchStatus.COMPLETED:
            succeeded = sum(1 for f in self.files if f.status == FileStatus.TRANSLATED)
            failed = sum(1 for f in self.files if f.status == FileStatus.FAILED)
            event = TranslationBatchCompleted(
                aggregate_id=self.batch_id,
                batch_id=self.batch_id,
                succeeded=succeeded,
                failed=failed,
            )
            return replace(
                self,
                status=BatchStatus.COMPLETED,
                _events=self._events + (event,),
            )
        return self

    # ── event harvesting ────────────────────────────────────────

    def collect_events(self) -> tuple[DomainEvent, ...]:
        """Return accumulated events."""
        return self._events

    def clear_events(self) -> Self:
        """Return a new instance with an empty events tuple."""
        return replace(self, _events=())
