from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class JobType(Enum):
    SPARK = "SPARK"
    FLINK = "FLINK"
    AIRFLOW_DAG = "AIRFLOW_DAG"
    LAMBDA = "LAMBDA"
    GLUE = "GLUE"
    EMR = "EMR"
    KAFKA_TOPIC = "KAFKA_TOPIC"


@dataclass(frozen=True)
class DependencyRef:
    source_path: str
    job_type: JobType
    references: tuple[str, ...]
    athena_queries: tuple[str, ...]
