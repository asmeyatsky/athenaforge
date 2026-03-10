from __future__ import annotations

from enum import Enum


class ProjectStatus(str, Enum):
    INITIALIZED = "initialized"
    SCAFFOLDING = "scaffolding"
    ACTIVE = "active"


class BatchStatus(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class FileStatus(str, Enum):
    PENDING = "pending"
    TRANSLATED = "translated"
    FAILED = "failed"


class TransferStatus(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class StreamingJobStatus(str, Enum):
    ACTIVE = "active"
    DRAINING = "draining"
    SWITCHING = "switching"
    VERIFIED = "verified"
    FAILED = "failed"
