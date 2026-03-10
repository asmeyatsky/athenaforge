from __future__ import annotations

from athenaforge.domain.events.dependency_events import (
    DAGRewriteCompleted,
    DependencyScanCompleted,
    IAMMappingGenerated,
    KafkaTopicMigrated,
    LambdaRewritten,
)
from athenaforge.domain.events.event_base import DomainEvent
from athenaforge.domain.events.foundation_events import (
    DataplexBootstrapCompleted,
    DeltaLogHealthChecked,
    ScaffoldGenerated,
    TierClassificationCompleted,
)
from athenaforge.domain.events.sql_events import (
    MapCascadeAnalysed,
    QueryValidationFailed,
    QueryValidationPassed,
    TranslationBatchCompleted,
    TranslationBatchStarted,
)
from athenaforge.domain.events.transfer_events import (
    DVTValidationCompleted,
    DeltaCompactionStarted,
    StreamingCutoverInitiated,
    TransferJobCompleted,
    TransferJobCreated,
)
from athenaforge.domain.events.wave_events import (
    ParallelRunModeChanged,
    RollbackTriggered,
    WaveCompleted,
    WaveGateFailed,
    WaveGatePassed,
    WavePlanned,
    WaveStarted,
)

__all__ = [
    # event_base
    "DomainEvent",
    # foundation_events
    "DataplexBootstrapCompleted",
    "DeltaLogHealthChecked",
    "ScaffoldGenerated",
    "TierClassificationCompleted",
    # sql_events
    "MapCascadeAnalysed",
    "QueryValidationFailed",
    "QueryValidationPassed",
    "TranslationBatchCompleted",
    "TranslationBatchStarted",
    # transfer_events
    "DVTValidationCompleted",
    "DeltaCompactionStarted",
    "StreamingCutoverInitiated",
    "TransferJobCompleted",
    "TransferJobCreated",
    # wave_events
    "ParallelRunModeChanged",
    "RollbackTriggered",
    "WaveCompleted",
    "WaveGateFailed",
    "WaveGatePassed",
    "WavePlanned",
    "WaveStarted",
    # dependency_events
    "DAGRewriteCompleted",
    "DependencyScanCompleted",
    "IAMMappingGenerated",
    "KafkaTopicMigrated",
    "LambdaRewritten",
]
