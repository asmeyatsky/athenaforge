from __future__ import annotations

import os
from uuid import uuid4

from athenaforge.application.dtos.sql_dtos import TranslationBatchResult
from athenaforge.domain.entities.translation_batch import TranslationBatch, TranslationFile
from athenaforge.domain.events.sql_events import (
    TranslationBatchCompleted,
    TranslationBatchStarted,
)
from athenaforge.domain.ports.event_bus import EventBusPort
from athenaforge.domain.ports.repository_ports import WriteRepositoryPort
from athenaforge.domain.ports.sql_translation_port import SqlTranslationPort
from athenaforge.domain.services.sql_pattern_matcher import SqlPatternMatcher


class TranslateBatchUseCase:
    """Orchestrates a full SQL translation batch through pattern matching and BQMS."""

    def __init__(
        self,
        pattern_matcher: SqlPatternMatcher,
        translation_port: SqlTranslationPort,
        batch_repo: WriteRepositoryPort,
        event_bus: EventBusPort,
    ) -> None:
        self._pattern_matcher = pattern_matcher
        self._translation_port = translation_port
        self._batch_repo = batch_repo
        self._event_bus = event_bus

    async def execute(
        self, source_dir: str, output_dir: str
    ) -> TranslationBatchResult:
        """Run translation on all SQL files found in *source_dir*.

        1. Discover source SQL files.
        2. Apply regex-based pattern pre-pass via ``SqlPatternMatcher``.
        3. Delegate to BQMS via ``SqlTranslationPort``.
        4. Persist the batch and publish domain events.
        """
        if '..' in source_dir:
            raise ValueError("Invalid source_dir: directory traversal not allowed")
        if '..' in output_dir:
            raise ValueError("Invalid output_dir: directory traversal not allowed")

        # Discover SQL files
        file_paths = [
            os.path.join(source_dir, f)
            for f in sorted(os.listdir(source_dir))
            if f.endswith(".sql")
        ]

        batch_id = uuid4().hex
        batch = TranslationBatch(
            batch_id=batch_id,
            files=tuple(TranslationFile(file_path=fp) for fp in file_paths),
        )

        # Publish started event
        await self._event_bus.publish(
            TranslationBatchStarted(
                aggregate_id=batch_id,
                batch_id=batch_id,
                file_count=len(file_paths),
            )
        )

        # Pre-pass: apply regex patterns to each file
        all_patterns_applied: list[str] = []
        pre_passed_paths: list[str] = []
        for fp in file_paths:
            with open(fp) as fh:
                sql = fh.read()
            rewritten, applied = self._pattern_matcher.apply_patterns(sql)
            all_patterns_applied.extend(applied)

            # Write pre-passed SQL to output_dir so the translator picks it up
            out_path = os.path.join(output_dir, os.path.basename(fp))
            os.makedirs(output_dir, exist_ok=True)
            with open(out_path, "w") as fh:
                fh.write(rewritten)
            pre_passed_paths.append(out_path)

        # Send to BQMS translation
        results = await self._translation_port.translate_batch(
            pre_passed_paths, output_dir
        )

        # Update batch entity from translation results
        for result in results:
            if result.success:
                batch = batch.mark_file_translated(
                    result.source_path,
                    result.translated_sql or result.source_path,
                )
            else:
                batch = batch.mark_file_failed(
                    result.source_path,
                    "; ".join(result.errors),
                )

        # Persist batch
        await self._batch_repo.save(batch)

        # Publish completed event
        succeeded = sum(1 for r in results if r.success)
        failed = len(results) - succeeded
        await self._event_bus.publish(
            TranslationBatchCompleted(
                aggregate_id=batch_id,
                batch_id=batch_id,
                succeeded=succeeded,
                failed=failed,
            )
        )

        # Publish any events accumulated on the aggregate
        for event in batch.collect_events():
            await self._event_bus.publish(event)

        return TranslationBatchResult(
            batch_id=batch_id,
            total_files=len(file_paths),
            succeeded=succeeded,
            failed=failed,
            patterns_applied=sorted(set(all_patterns_applied)),
        )
